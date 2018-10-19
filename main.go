package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/olivere/elastic"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Airport struct {
	IATA      string `json:"iata"`
	Name      string `json:"name"`
	City      string `json:"city"`
	Country   string `json:"country"`
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
	Timezone  string `json:"-"`
}

type Flight struct {
	Airline                      string  `json:"airline"`
	Carrier                      string  `json:"carrier"`
	Tail                         *string `json:"tail"`
	Number                       string  `json:"number"`
	Origin                       string  `json:"origin"`
	OriginGeo                    string  `json:"origin_geo"`
	OriginName                   string  `json:"origin_name"`
	OriginCity                   string  `json:"origin_city"`
	OriginCountry                string  `json:"origin_country"`
	Destination                  string  `json:"destination"`
	DestinationGeo               string  `json:"destination_geo"`
	DestinationName              string  `json:"destination_name"`
	DestinationCity              string  `json:"destination_city"`
	DestinationCountry           string  `json:"destination_country"`
	ScheduledDepTime             string  `json:"scheduled_departure_time"`
	ActualDepTime                *string `json:"actual_departure_time"`
	DepDelayMin                  *int    `json:"dep_delay_min"`
	TaxiOutMin                   *int    `json:"taxi_out_min"`
	TaxiInMin                    *int    `json:"taxi_in_min"`
	ScheduledArrTime             *string `json:"scheduled_arrival_time"`
	ActualArrTime                *string `json:"actual_arrival_time"`
	ArrDelayMin                  *int    `json:"arrival_delay_min"`
	Canceled                     bool    `json:"canceled"`
	CancelationReason            *string `json:"cancelation_reason"`
	Diverted                     bool    `json:"diverted"`
	ScheduledElapsedMin          *int    `json:"scheduled_elapsed_min"`
	ActualElapsedMin             *int    `json:"actual_elapsed_min"`
	AirTimeMin                   *int    `json:"air_time_min"`
	FlightSegments               int     `json:"flight_segments"`
	DistanceBetweenAirportsMiles int     `json:"distance_between_airports_miles"`
	CarrierDelayMin              *int    `json:"carrier_delay_min"`
	WeatherDelayMin              *int    `json:"weather_delay_min"`
	NationalAirSystemDelayMin    *int    `json:"national_air_system_delay_min"`
	SecurityDelayMin             *int    `json:"security_delay_min"`
	LateAircraftDelayMin         *int    `json:"late_aircraft_delay_min"`
}

// checkforErrors is invoked by bulk processor after every commit.
// The err variable indicates success or failure.
func checkForErrors(executionId int64,
	requests []elastic.BulkableRequest,
	response *elastic.BulkResponse,
	err error) {
	if err != nil {
		log.Error().
			Str("error", err.Error()).
			Msg("Bulk Insert Error")
		os.Exit(1)
	}
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	log.Info().Msg("Elastic loader ... starting")

	deleteFlag := flag.Bool("delete", false, "Delete index")
	flag.Parse()

	ctx := context.Background()

	// Elastic Client
	elasticEndpoint := os.Getenv("ELASTIC_ENDPOINT")
	elasticUsername := os.Getenv("ELASTIC_USERNAME")
	elasticPassword := os.Getenv("ELASTIC_PASSWORD")
	log.Info().Str("url", elasticEndpoint).Msg("Elastic server")
	elasticClient, err := elastic.NewClient(elastic.SetURL(elasticEndpoint),
		elastic.SetSniff(false),
		elastic.SetBasicAuth(elasticUsername, elasticPassword))
	if err != nil {
		log.Error().Str("error", err.Error()).Msg("Error initializing Elastic client")
		os.Exit(1)
	}
	log.Info().Msg("Initialized Elastic client")

	indexName := "flights"

	// Delete
	if *deleteFlag {
		indices := []string{indexName}
		for _, indexName := range indices {
			destroyIndex, err := elasticClient.DeleteIndex(indexName).Do(ctx)
			if err != nil {
				log.Error().
					Str("index", indexName).
					Str("error", err.Error()).
					Msg("Failed to delete Elastic index")
				os.Exit(1)
			} else {
				if !destroyIndex.Acknowledged {
					log.Error().
						Str("index", indexName).
						Str("error", err.Error()).
						Msg("Failed to acknowledge deletion of Elastic index")
					os.Exit(1)
				} else {
					log.Warn().
						Str("index", indexName).
						Msg("Index deleted")
				}
			}
		}
		os.Exit(0)
	}

	// Check for Index, create if necessary
	exists, err := elasticClient.IndexExists(indexName).Do(ctx)
	if err != nil {
		log.Error().
			Str("index", indexName).
			Str("error", err.Error()).
			Msg("Failed to see if index exists")
		os.Exit(1)
	}
	if !exists {
		mapping, err := ioutil.ReadFile("mapping.json")
		createIndex, err := elasticClient.CreateIndex(indexName).
			BodyString(string(mapping)).Do(ctx)
		if err != nil {
			log.Error().
				Str("index", indexName).
				Str("error", err.Error()).
				Msg("Error loading mapping")
			os.Exit(1)
		} else {
			if !createIndex.Acknowledged {
				log.Error().
					Str("index", indexName).
					Str("error", err.Error()).
					Msg("Create index not acknowledged")
				os.Exit(1)
			}
			log.Warn().
				Str("index", indexName).
				Msg("Index created")
		}
	} else {
		log.Warn().
			Str("index", indexName).
			Msg("Index already exists")
	}

	// Bulk Processor
	bulkProc, err := elasticClient.
		BulkProcessor().
		Name("Worker").
		Workers(4).
		After(checkForErrors).
		Do(context.Background())

	// Data Dir
	dataDir := fmt.Sprintf("%s/%s", os.Getenv("HOME"), "data/flights")

	// Airlines
	airlines := make(map[string]string)
	airlineFile := "airlines.csv"

	log.Debug().
		Str("name", airlineFile).
		Msg("Parsing airlines")
	csvFile, _ := os.Open(fmt.Sprintf("%s/%s", dataDir, airlineFile))
	reader := csv.NewReader(bufio.NewReader(csvFile))

	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Error().
				Str("error", err.Error()).
				Msg("Reading Airlines CSV file")
			os.Exit(1)
		}

		// Some airlines do not have an IATA code.
		airlines[line[3]] = line[1]
	}
	log.Debug().
		Int("count", len(airlines)).
		Msg("Airlines count")

	// Airports
	airports := make(map[string]Airport)
	airportFile := "airports.csv"

	log.Debug().
		Str("name", airportFile).
		Msg("Parsing airports")
	csvFile, _ = os.Open(fmt.Sprintf("%s/%s", dataDir, airportFile))
	reader = csv.NewReader(bufio.NewReader(csvFile))

	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Error().
				Str("error", err.Error()).
				Msg("Reading Airport CSV file")
			os.Exit(1)
		}

		iataCode := line[4]
		airports[iataCode] = Airport{
			IATA:      iataCode,
			Name:      line[1],
			City:      line[2],
			Country:   line[3],
			Latitude:  line[6],
			Longitude: line[7],
			Timezone:  line[11],
		}
	}
	log.Debug().
		Int("count", len(airports)).
		Msg("Airports count")

	// Trips
	flightFiles := []string{
		//"sample.csv",
		"2017-01.csv",
		"2017-02.csv",
		"2017-03.csv",
		"2017-04.csv",
		"2017-05.csv",
		"2017-06.csv",
		"2017-07.csv",
		"2017-08.csv",
		"2017-09.csv",
		"2017-10.csv",
		"2017-11.csv",
		"2017-12.csv",
		"2018-01.csv",
		"2018-02.csv",
		"2018-03.csv",
		"2018-04.csv",
		"2018-05.csv",
		"2018-06.csv",
		"2018-07.csv",
	}

	for _, flightFile := range flightFiles {
		log.Debug().
			Str("name", flightFile).
			Msg("Parsing file")
		csvFile, _ := os.Open(fmt.Sprintf("%s/%s", dataDir, flightFile))
		reader := csv.NewReader(bufio.NewReader(csvFile))

		for {
			line, err := reader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Error().
					Str("error", err.Error()).
					Msg("Reading Flight CSV file")
				os.Exit(1)
			}
			if line[0] == "FL_DATE" {
				continue
			}

			id := fmt.Sprintf("%s.%s.%s%s.%s.%s.%s", line[0], line[6], line[1], line[3],
				line[2], line[4], line[5])

			flight := Flight{}
			if airline, ok := airlines[line[1]]; ok {
				flight.Airline = airline
			}
			flight.Carrier = line[1]
			if line[2] != "" {
				t := line[2]
				flight.Tail = &t
			}
			flight.Number = line[3]
			if origin, ok := airports[line[4]]; ok {
				flight.Origin = origin.IATA
				flight.OriginGeo = fmt.Sprintf("%s,%s", origin.Latitude, origin.Longitude)
				flight.OriginName = origin.Name
				flight.OriginCity = origin.City
				flight.OriginCountry = origin.Country
				loc, err := time.LoadLocation(origin.Timezone)
				if err != nil {
					log.Error().
						Str("id", id).
						Str("error", err.Error()).
						Msg(fmt.Sprintf("Error getting timezone of %s", origin.Timezone))
					os.Exit(1)
				}
				// Date Examples:
				// 2017-01-31
				// 1447 or 0600 (24 time, zero padded)
				if line[6] != "" {
					isoTime := fmt.Sprintf("%s %s:%s %s",
						line[0], line[6][:2], line[6][2:],
						time.Now().In(loc).Format("-0700 MST"))
					parsedTime, err := time.Parse("2006-01-02 15:04 -0700 MST", isoTime)
					if err != nil {
						log.Error().
							Str("id", id).
							Str("error", err.Error()).
							Msg("Error parsing Sched Dep Time into Time")
						os.Exit(1)
					}
					flight.ScheduledDepTime = parsedTime.Format(time.RFC3339)
				}

				if line[7] != "" {
					if line[7] == "2400" {
						line[7] = "2359"
					}
					isoTime := fmt.Sprintf("%s %s:%s %s",
						line[0], line[7][:2], line[7][2:],
						time.Now().In(loc).Format("-0700 MST"))
					parsedTime, err := time.Parse("2006-01-02 15:04 -0700 MST", isoTime)
					if err != nil {
						log.Error().
							Str("id", id).
							Str("error", err.Error()).
							Msg("Error parsing Actual Dep Time into Time")
						os.Exit(1)
					}
					t := parsedTime.Format(time.RFC3339)
					flight.ActualDepTime = &t
				}
			} else {
				log.Error().
					Str("id", id).
					Msg("Error getting origin airport")
				os.Exit(1)
			}
			if dest, ok := airports[line[5]]; ok {
				flight.Destination = dest.IATA
				flight.DestinationGeo = fmt.Sprintf("%s,%s", dest.Latitude, dest.Longitude)
				flight.DestinationName = dest.Name
				flight.DestinationCity = dest.City
				flight.DestinationCountry = dest.Country
				loc, err := time.LoadLocation(dest.Timezone)
				if err != nil {
					log.Error().
						Str("id", id).
						Str("error", err.Error()).
						Msg(fmt.Sprintf("Error getting timezone of %s", dest.Timezone))
					os.Exit(1)
				}
				// Date Examples:
				// 2017-01-31
				// 1447 or 0600 (24 time, zero padded)
				if line[11] != "" {
					if line[11] == "2400" {
						line[11] = "2359"
					}
					isoTime := fmt.Sprintf("%s %s:%s %s",
						line[0], line[11][:2], line[11][2:],
						time.Now().In(loc).Format("-0700 MST"))
					parsedTime, err := time.Parse("2006-01-02 15:04 -0700 MST", isoTime)
					if err != nil {
						log.Error().
							Str("id", id).
							Str("error", err.Error()).
							Msg("Error parsing Sched Arr Time into Time")
						os.Exit(1)
					}
					t := parsedTime.Format(time.RFC3339)
					flight.ScheduledArrTime = &t
				}

				if line[12] != "" {
					if line[12] == "2400" {
						line[12] = "2359"
					}
					isoTime := fmt.Sprintf("%s %s:%s %s",
						line[0], line[12][:2], line[12][2:],
						time.Now().In(loc).Format("-0700 MST"))
					parsedTime, err := time.Parse("2006-01-02 15:04 -0700 MST", isoTime)
					if err != nil {
						log.Error().
							Str("id", id).
							Str("error", err.Error()).
							Msg("Error parsing Actual Arr Time into Time")
						os.Exit(1)
					}
					t := parsedTime.Format(time.RFC3339)
					flight.ActualArrTime = &t
				}
			} else {
				log.Error().
					Str("id", id).
					Msg("Error getting destination airport")
				os.Exit(1)
			}

			if line[8] != "" {
				depDelayMin, err := strconv.ParseFloat(line[8], 64)
				if err != nil {
					log.Error().
						Str("id", id).
						Str("error", err.Error()).
						Msg("Converting Dep Delay Min to float")
					os.Exit(1)
				}
				m := int(depDelayMin)
				flight.DepDelayMin = &m
			}

			if line[9] != "" {
				taxiOutMin, err := strconv.ParseFloat(line[9], 64)
				if err != nil {
					log.Error().
						Str("id", id).
						Str("error", err.Error()).
						Msg("Converting Taxi Out Min to int")
					os.Exit(1)
				}
				m := int(taxiOutMin)
				flight.TaxiOutMin = &m
			}

			if line[10] != "" {
				taxiInMin, err := strconv.ParseFloat(line[10], 64)
				if err != nil {
					log.Error().
						Str("id", id).
						Str("error", err.Error()).
						Msg("Converting Taxi In Min to int")
					os.Exit(1)
				}
				m := int(taxiInMin)
				flight.TaxiInMin = &m
			}

			if line[13] != "" {
				arrDelayMin, err := strconv.ParseFloat(line[13], 64)
				if err != nil {
					log.Error().
						Str("id", id).
						Str("error", err.Error()).
						Msg("Converting Arr Delay Min to int")
					os.Exit(1)
				}
				m := int(arrDelayMin)
				flight.ArrDelayMin = &m
			}

			canceled, err := strconv.ParseFloat(line[14], 64)
			if err != nil {
				log.Error().
					Str("id", id).
					Str("error", err.Error()).
					Msg("Converting Canceled to float")
				os.Exit(1)
			}
			c, err := strconv.ParseBool(fmt.Sprintf("%.0f", canceled))
			if err != nil {
				log.Error().
					Str("id", id).
					Str("error", err.Error()).
					Msg("Converting Canceled to bool")
				os.Exit(1)
			}
			flight.Canceled = c

			if line[15] == "A" {
				s := "Carrier"
				flight.CancelationReason = &s
			} else if line[15] == "B" {
				s := "Weather"
				flight.CancelationReason = &s
			} else if line[15] == "C" {
				s := "National Air System"
				flight.CancelationReason = &s
			} else if line[15] == "D" {
				s := "Security"
				flight.CancelationReason = &s
			}

			diverted, err := strconv.ParseFloat(line[16], 64)
			if err != nil {
				log.Error().
					Str("id", id).
					Str("error", err.Error()).
					Msg("Converting Diverted to float")
				os.Exit(1)
			}
			d, err := strconv.ParseBool(fmt.Sprintf("%.0f", diverted))
			if err != nil {
				log.Error().
					Str("id", id).
					Str("error", err.Error()).
					Msg("Converting Diverted to bool")
				os.Exit(1)
			}
			flight.Diverted = d

			if line[17] != "" {
				schElapsedMin, err := strconv.ParseFloat(line[17], 64)
				if err != nil {
					log.Error().
						Str("id", id).
						Str("error", err.Error()).
						Msg("Converting Sched Elapsed Min to int")
					os.Exit(1)
				}
				m := int(schElapsedMin)
				flight.ScheduledElapsedMin = &m
			}

			if line[18] != "" {
				actElapsedMin, err := strconv.ParseFloat(line[18], 64)
				if err != nil {
					log.Error().
						Str("id", id).
						Str("error", err.Error()).
						Msg("Converting Act Elapsed Min to int")
					os.Exit(1)
				}
				m := int(actElapsedMin)
				flight.ActualElapsedMin = &m
			}

			if line[19] != "" {
				airTimeMin, err := strconv.ParseFloat(line[19], 64)
				if err != nil {
					log.Error().
						Str("id", id).
						Str("error", err.Error()).
						Msg("Converting Air Time Min to int")
					os.Exit(1)
				}
				m := int(airTimeMin)
				flight.AirTimeMin = &m
			}

			flightSegments, err := strconv.ParseFloat(line[20], 64)
			if err != nil {
				log.Error().
					Str("id", id).
					Str("error", err.Error()).
					Msg("Converting Flight Segments to int")
				os.Exit(1)
			}
			flight.FlightSegments = int(flightSegments)

			distance, err := strconv.ParseFloat(line[21], 64)
			if err != nil {
				log.Error().
					Str("id", id).
					Str("error", err.Error()).
					Msg("Converting Distance Btwn Airports to int")
				os.Exit(1)
			}
			flight.DistanceBetweenAirportsMiles = int(distance)

			if line[22] != "" {
				carrierDelayMin, err := strconv.ParseFloat(line[22], 64)
				if err != nil {
					log.Error().
						Str("id", id).
						Str("error", err.Error()).
						Msg("Converting Carrier Delay Min to int")
					os.Exit(1)
				}
				m := int(carrierDelayMin)
				flight.CarrierDelayMin = &m
			}

			if line[23] != "" {
				weatherDelayMin, err := strconv.ParseFloat(line[23], 64)
				if err != nil {
					log.Error().
						Str("id", id).
						Str("error", err.Error()).
						Msg("Converting Weather Delay Min to int")
					os.Exit(1)
				}
				m := int(weatherDelayMin)
				flight.WeatherDelayMin = &m
			}

			if line[24] != "" {
				nasDelayMin, err := strconv.ParseFloat(line[24], 64)
				if err != nil {
					log.Error().
						Str("id", id).
						Str("error", err.Error()).
						Msg("Converting NAS Delay Min to int")
					os.Exit(1)
				}
				m := int(nasDelayMin)
				flight.NationalAirSystemDelayMin = &m
			}

			if line[25] != "" {
				secDelayMin, err := strconv.ParseFloat(line[25], 64)
				if err != nil {
					log.Error().
						Str("id", id).
						Str("error", err.Error()).
						Msg("Converting Sec Delay Min to int")
					os.Exit(1)
				}
				m := int(secDelayMin)
				flight.SecurityDelayMin = &m
			}

			if line[26] != "" {
				laDelayMin, err := strconv.ParseFloat(line[26], 64)
				if err != nil {
					log.Error().
						Str("id", id).
						Str("error", err.Error()).
						Msg("Converting Late Aircraft Delay Min to int")
					os.Exit(1)
				}
				m := int(laDelayMin)
				flight.LateAircraftDelayMin = &m
			}

			jsonFlight, err := json.Marshal(flight)
			if err != nil {
				log.Error().
					Str("error", err.Error()).
					Msg("Error marshalling JSON")
				os.Exit(1)
			} else {
				// 2017-01-31.1200.AA123.N1234.ORD.SFO
				indexRequest := elastic.NewBulkIndexRequest().
					Index(indexName).
					Type("_doc").
					OpType("create").
					Id(id).
					Doc(string(jsonFlight))
				bulkProc.Add(indexRequest)
			}
		}
	}

	err = bulkProc.Flush()
	if err != nil {
		log.Error().
			Str("error", err.Error()).
			Msg("Flushing Bulk Processor")
		os.Exit(1)
	}
}
