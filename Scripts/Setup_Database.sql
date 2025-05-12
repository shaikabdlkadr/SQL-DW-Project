-- Setting up the Database and Schemas for the project

/*
==========================================================================================================================================
Setup_Database Script : Creating the database and schemas for the data warehousing project
==========================================================================================================================================

Purpose:
   - This script creates a new database named 'DataWarehouse' after checking if it already exists, it is dropped and recreated.
   - Additionally, the script sets up three schemas within the database.
   - Schemas named as, 'bronze', 'silver', and 'gold'.

WARNING:
   - Running this script will drop the entire 'datawarehouse' database if it exists. 
   - All data in the database will be permanently deleted. Proceed with caution.
   - To prevent any failures, ensure you have proper backups before running this script.
==========================================================================================================================================
*/

-- Check wheather the database is exists
SELECT datname AS "Database EXISTS"
FROM pg_catalog.pg_database WHERE datname = 'data_warehouse';

-- if present, dropping the database
DROP DATABASE IF EXISTS data_warehouse WITH (FORCE);


-- Database creation with DB user as OWNER with comments
CREATE DATABASE data_warehouse
    WITH
    OWNER = "<DB User Name>"
    ENCODING = 'UTF8'
    LOCALE_PROVIDER = 'libc'
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

COMMENT ON DATABASE data_warehouse
    IS '<your own comment : optional, we can ignore>';


-- Creating SCHEMA for the bronze, silver and gold layer

CREATE SCHEMA IF NOT EXISTS bronze
    AUTHORIZATION "SQLTools";

COMMENT ON SCHEMA "bronze"
    IS 'Medallion Architecture : Bronze layer.';


CREATE IF NOT EXISTS SCHEMA "silver"
    AUTHORIZATION "SQLTools";

COMMENT ON SCHEMA "silver"
    IS 'Medallion Architecture : silver layer and tranasformation and data standardization will take place here.';


CREATE IF NOT EXISTS SCHEMA "gold"
    AUTHORIZATION "SQLTools";

COMMENT ON SCHEMA "gold"
    IS 'Medallion Architecture : gold layer, expxorting the layer to used for reporting and analytics, end users.';