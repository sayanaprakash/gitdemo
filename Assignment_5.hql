use hiveDB;

--Create external EmpAnalysis table
drop table if exists EmpAnalysis;

CREATE EXTERNAL TABLE IF NOT EXISTS EmpAnalysis (EmployeeID INT, Department VARCHAR(50), 
JobRole VARCHAR(50), Attrition VARCHAR(10), Gender VARCHAR(10), Age INT, MaritalStatus VARCHAR(30), 
Education VARCHAR(50), EducationField VARCHAR(70), BusinessTravel VARCHAR(30), JobInvolvement 
VARCHAR(20), JobLevel INT, JobSatisfaction VARCHAR(30), Hourlyrate INT, Income INT, Salaryhike INT, 
OverTime VARCHAR(20), Workex INT, YearsSinceLastPromotion INT,EmpSatisfaction VARCHAR(20),
TrainingTimesLastYear INT, WorkLifeBalance VARCHAR(30),Performance_Rating VARCHAR(40))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS textfile
location '/hive/Assignment4';

--select * from EmpAnalysis limit 5;

--1. Create a “parquet” table using HR data files.

drop table if exists parquet_EmpAnalysis;

CREATE EXTERNAL TABLE IF NOT EXISTS parquet_EmpAnalysis (EmployeeID INT, Department VARCHAR(50), 
JobRole VARCHAR(50), Attrition VARCHAR(10), Gender VARCHAR(10), Age INT, MaritalStatus VARCHAR(30), 
Education VARCHAR(50), EducationField VARCHAR(70), BusinessTravel VARCHAR(30), JobInvolvement 
VARCHAR(20), JobLevel INT, JobSatisfaction VARCHAR(30), Hourlyrate INT, Income INT, Salaryhike INT, 
OverTime VARCHAR(20), Workex INT, YearsSinceLastPromotion INT,EmpSatisfaction VARCHAR(20), 
TrainingTimesLastYear INT, WorkLifeBalance VARCHAR(30),Performance_Rating VARCHAR(40))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS parquetfile;

--Insert data from external table to partquet table
insert into table parquet_EmpAnalysis
select * from EmpAnalysis;

--Display First 5 Rows
select * from parquet_EmpAnalysis limit 5;
