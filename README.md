## Airline Flights

The United States Department of Transportation has Flight Stats available through the Bureau of Transportation Statistics.

[https://transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time](https://transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time)

The fields we use are specified in detail further below, but here is a sample record:

|"FL\_DATE"|"OP\_CARRIER"|"TAIL\_NUM"|"OP\_CARRIER\_FL\_NUM"|"ORIGIN"|"DEST"|"CRS\_DEP\_TIME"|"DEP\_TIME"|"DEP\_DELAY"|"TAXI\_OUT"|"TAXI\_IN"|"CRS\_ARR\_TIME"|"ARR\_TIME"|"ARR\_DELAY"|"CANCELLED"|"CANCELLATION\_CODE"|"DIVERTED"|"CRS\_ELAPSED\_TIME"|"ACTUAL\_ELAPSED\_TIME"|"AIR\_TIME"|"FLIGHTS"|"DISTANCE"|"CARRIER\_DELAY"|"WEATHER\_DELAY"|"NAS\_DELAY"|"SECURITY\_DELAY"|"LATE\_AIRCRAFT\_DELAY"|
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|2017-01-01|"AA"|"N787AA"|"1"|"JFK"|"LAX"|"0800"|"0831"|31.00|25.00|26.00|"1142"|"1209"|27.00|0.00|""|0.00|402.00|398.00|347.00|1.00|2475.00|27.00|0.00|0.00|0.00|0.00|

Objective:

1. Ask questions that can be answered by the data.
2. Answer the questions in Kibana.
3. Find surprises in the data.

For example:

* What's the busiest airport?
* How many flights were there in 2017?
* What was the most popular holiday to fly?
* What aircraft made the most flights?
* What airport has the most delays (or the least)?

Write down other questions you have so we can answer them with Elastic.

### Ingest

Getting this data into Elastic can be accomplished using:

* Logstash
* Beats
* Programming Language

At its core, data is ingested via the [Document APIs](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs.html).  These are a set of RESTful APIs that all of the methods above use to ingest data.  It's recommended you use whatever tool (or language) you are most comfortable with.  Logstash & Beats provide a configuration-driven approach to ingesting data, while a programming langauge will give you more flexibility at the cost of verbosity.  There are tradeoffs to each approach but the choice is yours.

Though Go is not part of the official [Elasticsearch Clients](https://www.elastic.co/guide/en/elasticsearch/client/index.html) supported by Elastic, there is a popular [Elastic Go library](https://github.com/olivere/elastic) that wraps the REST APIs.  We will be using that library to ingest data.

### Data Sources

To get the data used for this exercise, select these data fields from the download form linked to above:

1. FlightDate
1. IATA_CODE_Reporting_Airline
1. Tail_Number
1. Flight_Number_Reporting_Airline
1. Origin
1. Dest
1. CRSDepTime (CRS Departure Time (local time: hhmm))  
1. DepTime (Actual Departure Time (local time: hhmm))
1. DepDelay (Difference in minutes between scheduled and actual departure time. Early departures show negative numbers.)
1. TaxiOut (Taxi Out Time, in Minutes)
1. TaxiIn (Taxi In Time, in Minutes)
1. CRSArrTime (CRS Arrival Time (local time: hhmm))
1. ArrTime (Actual Arrival Time (local time: hhmm))
1. ArrDelay (Difference in minutes between scheduled and actual arrival time. Early arrivals show negative numbers.)
1. Cancelled (Cancelled Flight Indicator, 1=Yes, 0=No)
1. CancellationCode (Specifies The Reason For Cancellation: "A","Carrier", "B","Weather", "C","National Air System", "D","Security")
1. Diverted (Diverted Flight Indicator, 1=Yes, 0=No)
1. CRSElapsedTime (CRS Elapsed Time of Flight, in Minutes)
1. ActualElapsedTime (Elapsed Time of Flight, in Minutes)
1. AirTime (Flight Time, in Minutes)
1. Flights (Number of Flights)
1. Distance (Distance between airports (miles))
1. CarrierDelay (Carrier Delay, in Minutes)
1. WeatherDelay (Weather Delay, in Minutes)
1. NASDelay (National Air System Delay, in Minutes)
1. SecurityDelay (Security Delay, in Minutes)
1. LateAircraftDelay (Late Aircraft Delay, in Minutes)

Then select each month & year you want data for and click download.  Unzip the file and rename it to "YEAR-MONTH.csv" (e.g., `2017-02.csv`).  Repeat this until you have all the months you want data for.

Download the Airport data to get each Airports latitude and longitude:

[https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat](https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat)

* Rename that file to `airports.csv`.
* Open vim to search and replace `\"` with nothing using: `:%s/\\"//g`
* Add the following lines to the top of that file:

`0,"Laughlin/Bullhead International Airport","Chicago","United States","IFP","KIFP",35.156111,-114.559444,707,-7,"N","America/Phoenix","airport","OurAirports"`
`0,"Stillwater Regional Airport","Stillwater","United States","SWO","KSWO",36.161111,-97.085556,1295,-6,"A","America/Chicago","airport","OurAirports"`
`0,"Concord Regional Airport","Concord","United States","USA","KJQF",35.387778,-80.709167,705,-5,"A","America/New_York","airport","OurAirports"`
`0,"Branson Airport","Branson","United States","BKG","KBBG",36.531944,-93.200556,1302,-6,"A","America/Chicago","airport","OurAirports"`

Download the Airline data to get each Airline's full name:

[https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat](https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat)

Rename that file to `airlines.csv`.

Put all these data files in the directory `~/data/flights`.

