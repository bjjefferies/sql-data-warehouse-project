/*
===================================================
CREATE DATABASE SCRIPT
===================================================
Script Purpose: 
  Creates a new database called 'DataWarehouse' and checks to see if one already exists. Since this is for practice, the script also
  checkes if a duplicate script already exists and drops it if so. The script also creates 3 schema within the database: 
  bronze, silver, gold.

WARNING:
  This script will permanently delete any database already existing with the name 'DataWarehouse'. Please ensure you have proper backups
  before running this script.

*/



-- Create Database 'DataWarehouse'

USE master; -- set path to master sqlserver
GO

-- Check for existing database and drop
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

  
CREATE DATABASE DataWarehouse;  -- create db

USE DataWarehouse;  -- set path to new db


-- Create Shema for 3 layers (bronze, silver, gold)

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
