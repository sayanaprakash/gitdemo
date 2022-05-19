-- Create Database
create database if not exists pokemonDB;

-- Use Database
use pokemonDB;

drop table if exists pokemon_tbl;

-- Create an External Table
create external table if not exists pokemon_tbl (Id int, Name string, Type_1 string, 
Type_2 string, Total int, HP int, Attack int, Defense int, Sp_Atk int,Sp_Def int, 
Speed int, Generation int, Legendary boolean) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n';

-- Load the Data into External Table
LOAD DATA LOCAL INPATH '/home/hadoop/Downloads/pokemon.csv' INTO TABLE pokemon_tbl;

-- Display the First Five Records
select * from pokemon_tbl limit 5;

