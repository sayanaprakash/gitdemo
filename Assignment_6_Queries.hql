use airline_delayDB;

--d) Average arrival delay caused by airlines

--select AIRLINE, avg(ARRIVAL_DELAY) as Average_of_Arrival_Delay from parquet_flights 
--group by AIRLINE order by Average_of_Arrival_Delay desc;

--e) Days of months with respected to average of arrival delays

--select MONTH, DAY, avg(ARRIVAL_DELAY) as AVG_Arrival_Delay from parquet_flights group by MONTH, DAY order by MONTH;

--f) Arrange weekdays with respect to the average arrival delays caused

--select DAY_OF_WEEK, avg(ARRIVAL_DELAY) as AVG_Arrival_Delay from parquet_flights group by DAY_OF_WEEK 
--order by DAY_OF_WEEK;

--g) Arrange Days of month as per cancellations done in Descending

--select MONTH, DAY, sum(CANCELLED) as Total_Cancellation from parquet_flights where CANCELLED = 1 group by MONTH, DAY 
--order by Total_Cancellation desc;

--h) Finding busiest airports with respect to day of week

--select DAY_OF_WEEK, count(ORIGIN_AIRPORT) as Origin_Count, count(DESTINATION_AIRPORT) as Destination_Count 
--from parquet_flights group by DAY_OF_WEEK order by Origin_Count desc, Destination_Count desc;

--i) Finding airlines that make the maximum number of cancellations

--select AIRLINE, sum(CANCELLED) as Total_Cancellation from parquet_flights where CANCELLED = 1 group by AIRLINE 
--order by Total_Cancellation desc;

--j) Find and order airlines in descending that make the most number of diversions

--select AIRLINE, sum(DIVERTED) as No_of_Diversions from parquet_flights where DIVERTED = 1 group by AIRLINE order by 
--No_of_Diversions desc;

--k) Finding days of month that see the most number of diversion

--select MONTH, DAY, sum(DIVERTED) as No_of_Diversions from parquet_flights where DIVERTED = 1 group by MONTH, DAY order by No_of_Diversions desc;

--l) Calculating mean and standard deviation of departure delay for all flights in minutes

--select AIRLINE, avg(DEPARTURE_DELAY) as mean, stddev(DEPARTURE_DELAY) as std_deviation from parquet_flights 
--group by AIRLINE order by mean desc;

--m) Calculating mean and standard deviation of arrival delay for all flights in minutes

--select AIRLINE, avg(ARRIVAL_DELAY) as mean, stddev(ARRIVAL_DELAY) as std_deviation from parquet_flights
--group by AIRLINE order by mean desc;

--n) Create a partitioning table “flights_partition” using partitioned by schema “CANCELLED”

drop table if exists flight;

CREATE EXTERNAL TABLE IF NOT EXISTS flight(ID int, YEAR int, MONTH INT,
DAY INT, DAY_OF_WEEK INT,AIRLINE VARCHAR(20),FLIGHT_NUMBER INT, TAIL_NUMBER VARCHAR(30),
ORIGIN_AIRPORT VARCHAR(20), DESTINATION_AIRPORT VARCHAR(20), SCHEDULED_DEPARTURE INT,
DEPARTURE_TIME INT,DEPARTURE_DELAY INT, TAXI_OUT INT, WHEELS_OFF INT,SCHEDULED_TIME INT,
ELAPSED_TIME INT, AIR_TIME INT,DISTANCE INT, WHEELS_ON INT, TAXI_IN INT, SCHEDULED_ARRIVAL INT,
ARRIVAL_TIME INT, ARRIVAL_DELAY INT,DIVERTED INT, CANCELLED INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
location '/hive/Assignment6';

--select * from flight limit 5;

--Set Hive Properties to Enable Dynamic Partition
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

--Create an External Partition Table
drop table if exists flights_partition;
CREATE EXTERNAL TABLE IF NOT EXISTS flights_partition(ID int, YEAR int, MONTH INT,
DAY INT, DAY_OF_WEEK INT, AIRLINE VARCHAR(20),FLIGHT_NUMBER INT, TAIL_NUMBER VARCHAR(30),
ORIGIN_AIRPORT VARCHAR(20), DESTINATION_AIRPORT VARCHAR(20), SCHEDULED_DEPARTURE INT,
DEPARTURE_TIME INT,DEPARTURE_DELAY INT, TAXI_OUT INT, WHEELS_OFF INT,SCHEDULED_TIME INT,
ELAPSED_TIME INT, AIR_TIME INT,DISTANCE INT, WHEELS_ON INT, TAXI_IN INT, SCHEDULED_ARRIVAL INT,
ARRIVAL_TIME INT, ARRIVAL_DELAY INT,DIVERTED INT)
partitioned by (CANCELLED INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

--Insert the Data into Partitioned Table 
insert overwrite table flights_partition
partition(CANCELLED)
select * from flight;

select * from flights_partition limit 5;

--o) Create Bucketing table “Flights_Bucket” using clustered by MONTH into 3 Buckets Note: No partitioning, 
--   only bucketing of table.

--set hive.exec.dynamic.partition.mode=nonstrict;
--set hive.enforce.bucketing=true;

--drop table if exists Flights_Bucket;

--create external table if not exists Flights_Bucket(ID INT, YEAR INT, MONTH int, DAY int, DAY_OF_WEEK int, AIRLINE
--varchar(4), FLIGHT_NUMBER int, TAIL_NUMBER varchar(10), ORIGIN_AIRPORT varchar(5), DESTINATION_AIRPORT
--varchar(5), SCHEDULED_DEPARTURE int, DEPARTURE_TIME int, DEPARTURE_DELAY int, TAXI_OUT int, WHEELS_OFF int,
--SCHEDULED_TIME int, ELAPSED_TIME int, AIR_TIME int,     DISTANCE int, WHEELS_ON int, TAXI_IN int, SCHEDULED_ARRIVAL
--int, ARRIVAL_TIME int, ARRIVAL_DELAY int, DIVERTED int, CANCELLED int, CANCELLATION_REASON varchar(2),
--AIR_SYSTEM_DELAY int, SECURITY_DELAY int, AIRLINE_DELAY int, LATE_AIRCRAFT_DELAY int, WEATHER_DELAY int)
--clustered by (MONTH) into 3 buckets
--row format delimited
--fields terminated by ','
--lines terminated by '\n'
--stored as textfile;

--describe formatted Flights_Bucket;

--Insert Bucket Employee Data
--insert overwrite table Flights_Bucket
--select * from flights;

--select * from Flights_Bucket limit 5;

--p) Get count of data of each bucket.

--select MONTH, count(*) as cnt from Flights_Bucket group by MONTH order by cnt;

--q) Finding all diverted Route from a source to destination Airport & which route is the most diverted

--select ORIGIN_AIRPORT, DESTINATION_AIRPORT, sum(DIVERTED) as Divertions from Flights_Bucket where 
--DIVERTED = 1 group by ORIGIN_AIRPORT, DESTINATION_AIRPORT order by Divertions desc;

--r) Finding AIRLINES with its total flight count, total number of flights arrival delayed by more than 30 
--   Minutes, % of such flights delayed by more than 30 minutes when it is not Weekends with minimum count of 
--   flights from Airlines by more than 10. Also Exclude some of Airlines 'AK', 'HI', 'PR', 'VI' and arrange 
--   output in descending order by % of such count of flights.

--select AIRLINE, total_flights, airline_delay_gt_30, not_weekend, not_weekend*100/total_flights as Percentage
--from (select AIRLINE, count(AIRLINE) as total_flights,
--sum(case when ARRIVAL_DELAY > 30 then 1 else 0 end) as airline_delay_gt_30,
--sum(case when ARRIVAL_DELAY > 30 and (DAY_OF_WEEK <> 6 and DAY_OF_WEEK <> 7) then 1 else 0 end) as not_weekend 
--from Flights_Bucket where AIRLINE not in ('AK', 'HI', 'PR', 'VI')
--group by AIRLINE having total_flights > 10) as sample order by Percentage desc;

--s) Finding AIRLINES with its total flight count with total number of flights departure delayed by less 
--   than 30 Minutes, % of such flights delayed by less than 30 minutes when it is Weekends with minimum 
--   count of flights from Airlines by more than 10. Also Exclude some of Airlines 'AK', 'HI', 'PR', 'VI' 
--   and arrange output in descending order by % of such count of flights. 

--select AIRLINE, total_flights, departure_delay_gt_30, not_weekend, not_weekend*100/total_flights as Percentage
--from (select AIRLINE, count(AIRLINE) as total_flights,
--sum(case when DEPARTURE_DELAY < 30 then 1 else 0 end) as departure_delay_gt_30,
--sum(case when DEPARTURE_DELAY < 30 and (DAY_OF_WEEK <> 6 and DAY_OF_WEEK <> 7) then 1 else 0 end) as not_weekend
--from Flights_Bucket where AIRLINE not in ('AK', 'HI', 'PR', 'VI')
--group by AIRLINE having total_flights > 10) as sample order by Percentage desc;

--t) When is the best time of day/day of week/time of a year to fly with minimum delays?

--select DAY_OF_WEEK, avg(ARRIVAL_DELAY) + avg(DEPARTURE_DELAY)  as AVG_Delays from parquet_flights 
--group by DAY_OF_WEEK order by AVG_Delays;
