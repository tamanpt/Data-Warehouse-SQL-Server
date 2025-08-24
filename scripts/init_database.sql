/*
Create Database and schema for Datawarehouse
*/
-- CREATE Database 'DataWarehouse'


USE MASTER;

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
