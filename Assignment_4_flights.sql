-- Create Database flightsDB
create database if not exists flightsDB;
use flightsDB;

-- 1. Create a Table Flights with schemas of Table

drop table if exists Flights;
create table if not exists Flights (ID int, YEAR int, MONTH int, 
DAY int, DAY_OF_WEEK int, AIRLINE varchar(4), FLIGHT_NUMBER int,
TAIL_NUMBER varchar(10), ORIGIN_AIRPORT varchar(5),	DESTINATION_AIRPORT varchar(5),	
SCHEDULED_DEPARTURE	int, DEPARTURE_TIME int, DEPARTURE_DELAY int, TAXI_OUT int,	
WHEELS_OFF int,	SCHEDULED_TIME int, ELAPSED_TIME int, AIR_TIME int,	DISTANCE int, 
WHEELS_ON int, TAXI_IN int, SCHEDULED_ARRIVAL int, ARRIVAL_TIME int, ARRIVAL_DELAY int,	
DIVERTED int, CANCELLED	int, CANCELLATION_REASON varchar(2), AIR_SYSTEM_DELAY int, 
SECURITY_DELAY int, AIRLINE_DELAY int, LATE_AIRCRAFT_DELAY int, WEATHER_DELAY int, PRIMARY KEY(ID));

-- 2. Insert all records into flights table. Use dataset Flights_Delay.csv. 
-- 	  Write a MySQL Queries to display the results

SET GLOBAL local_infile = true;

LOAD DATA LOCAL INFILE 'E:/Data Science/MySQL/Flights_Delay.csv' INTO TABLE Flights
FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

-- Display Top 7 Rows 
select * from Flights limit 7;

-- 3. Average Arrival delay caused by airlines

select AIRLINE, avg(ARRIVAL_DELAY) as Average_of_Arrival_Delay from Flights 
group by AIRLINE order by Average_of_Arrival_Delay desc;

-- 4. Display the Day of Month with AVG Delay [Hint: Add Count() of Arrival & Departure Delay]

select MONTH, DAY, avg(ARRIVAL_DELAY) as AVG_Arrival_Delay, avg(DEPARTURE_DELAY) as AVG_Departure_Delay,
avg(ARRIVAL_DELAY) + avg(DEPARTURE_DELAY) as AVG_Delay from Flights group by MONTH, DAY order by MONTH;

-- 5. Analysis for each month with total number of cancellations.

select MONTH, sum(CANCELLED) as Total_Cancellation from Flights group by MONTH order by MONTH;

-- 6. Find the airlines that make maximum number of cancellations

select AIRLINE, sum(CANCELLED) as Total_Cancellation from Flights group by AIRLINE 
order by Total_Cancellation desc;

-- 7. Finding the Busiest Airport [Hint: Find Count() of origin airport and destination airport]

select ORIGIN_AIRPORT, DESTINATION_AIRPORT, count(ORIGIN_AIRPORT) as Origin_Count, 
count(DESTINATION_AIRPORT) as Destination_Count from Flights group by ORIGIN_AIRPORT, 
DESTINATION_AIRPORT order by Origin_Count desc, Destination_Count desc;

-- 8. Find the airlines that make maximum number of Diversions [Hint: Diverted = 1 indicate Diversion]

select AIRLINE, sum(DIVERTED) as No_of_Diversions from Flights group by AIRLINE order by No_of_Diversions desc;

-- 9. Finding all diverted Route from a source to destination Airport & which route is the most diverted route.

select ORIGIN_AIRPORT, DESTINATION_AIRPORT, sum(DIVERTED) as Divertions from Flights where DIVERTED = 1 group by 
ORIGIN_AIRPORT order by Divertions desc;

-- 10. Finding all Route from origin to destination Airport & which route got delayed.

select ORIGIN_AIRPORT, DESTINATION_AIRPORT, AIRLINE_DELAY as Delay from Flights group by 
ORIGIN_AIRPORT, DESTINATION_AIRPORT order by Delay desc;

-- 11. Finding the Route which Got Delayed the Most [Hint: Route include Origin Airport and Destination Airport, Group By Both]

select ORIGIN_AIRPORT, DESTINATION_AIRPORT, AIRLINE_DELAY as Delay from Flights group by 
ORIGIN_AIRPORT, DESTINATION_AIRPORT order by Delay desc;

-- 12. Finding AIRLINES with its total flight count, total number of flights arrival delayed by more than 30 Minutes, 
--     % of such flights delayed by more than 30 minutes when it is not Weekends with minimum count of flights from 
--     Airlines by more than 10. Also Exclude some of Airlines 'AK', 'HI', 'PR', 'VI' and arrange output in 
--     descending order by % of such count of flights.

select AIRLINE, total_flights, airline_delay_gt_30, not_weekend, not_weekend*100/total_flights as Percentage
from (select AIRLINE, count(AIRLINE) as total_flights,
sum(case when ARRIVAL_DELAY > 30 then 1 else 0 end) as airline_delay_gt_30,
sum(case when ARRIVAL_DELAY > 30 and (DAY_OF_WEEK <> 6 and DAY_OF_WEEK <> 7) then 1 else 0 end) as not_weekend 
from Flights where AIRLINE not in ('AK', 'HI', 'PR', 'VI')
group by AIRLINE having total_flights > 10) as sample order by Percentage desc;
