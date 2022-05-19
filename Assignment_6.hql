use airline_delayDB;

--a) Create external table “flights” using Database “airline_delayDB”

drop table if exists flights;

CREATE EXTERNAL TABLE IF NOT EXISTS flights (ID INT, YEAR INT, MONTH int, DAY int, DAY_OF_WEEK int, AIRLINE 
varchar(4), FLIGHT_NUMBER int, TAIL_NUMBER varchar(10), ORIGIN_AIRPORT varchar(5), DESTINATION_AIRPORT 
varchar(5), SCHEDULED_DEPARTURE int, DEPARTURE_TIME int, DEPARTURE_DELAY int, TAXI_OUT int, WHEELS_OFF int, 
SCHEDULED_TIME int, ELAPSED_TIME int, AIR_TIME int,	DISTANCE int, WHEELS_ON int, TAXI_IN int, SCHEDULED_ARRIVAL 
int, ARRIVAL_TIME int, ARRIVAL_DELAY int, DIVERTED int, CANCELLED int, CANCELLATION_REASON varchar(2), 
AIR_SYSTEM_DELAY int, SECURITY_DELAY int, AIRLINE_DELAY int, LATE_AIRCRAFT_DELAY int, WEATHER_DELAY int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS textfile
location '/hive/Assignment6';

--select * from flights limit 5;

--b) Create a parquet table “parquet_flights” & insert the data into this using “flights” external table

drop table if exists parquet_flights;

CREATE EXTERNAL TABLE IF NOT EXISTS parquet_flights (ID INT, YEAR INT, MONTH int, DAY int, DAY_OF_WEEK int, AIRLINE
varchar(4), FLIGHT_NUMBER int, TAIL_NUMBER varchar(10), ORIGIN_AIRPORT varchar(5), DESTINATION_AIRPORT
varchar(5), SCHEDULED_DEPARTURE int, DEPARTURE_TIME int, DEPARTURE_DELAY int, TAXI_OUT int, WHEELS_OFF int,
SCHEDULED_TIME int, ELAPSED_TIME int, AIR_TIME int,   DISTANCE int, WHEELS_ON int, TAXI_IN int, SCHEDULED_ARRIVAL
int, ARRIVAL_TIME int, ARRIVAL_DELAY int, DIVERTED int, CANCELLED int, CANCELLATION_REASON varchar(2),
AIR_SYSTEM_DELAY int, SECURITY_DELAY int, AIRLINE_DELAY int, LATE_AIRCRAFT_DELAY int, WEATHER_DELAY int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS parquetfile;

--Insert data from external table to partquet table
insert into table parquet_flights
select * from flights;

--c) Describe the table schema & show top 10 rows of Dataset

describe parquet_flights;

select * from parquet_flights limit 10;

