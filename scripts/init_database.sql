-- 1 Data Warehouse Initialization Script
-- Project:  Data Warehouse System
-- Author: Hervin

-- Objective:
-- This script creates a Data Warehouse using Medallion Architecture
-- (Bronze → Silver → Gold) to organize data from raw to analytics-ready format


--  Step 1: Use master database to create a new database
USE master;
GO

-- Step 2: Create Data Warehouse Database
CREATE DATABASE Datawarehouse;
GO

-- Step 3: Switch to the newly created database
USE Datawarehouse;
GO


--  Bronze Layer (Raw Data Layer)
--  Stores raw, unprocessed data from source systems (CSV, APIs, Logs)
--  No transformations applied (as-is data)
CREATE SCHEMA bronze;
GO


--  Silver Layer (Cleaned & Transformed Data)
--  Data is cleaned, validated, and standardized
--  Removes duplicates, handles nulls, applies business rules
CREATE SCHEMA silver;
GO


--  Gold Layer (Business / Analytics Layer)
--  Contains aggregated and business-ready data
--  Used for reporting, dashboards (Power BI), and analytics
CREATE SCHEMA gold;
GO


-- Summary:
-- Bronze  → Raw Data Ingestion Layer  
-- Silver  → Cleaned & Transformed Layer
-- Gold    → Analytics & Reporting Layer

-- This architecture improves:
-- - Data quality
-- - Scalability
-- - Maintainability
-- - Performance for analytics queries

-- 💡 Inspired by modern Data Engineering practices (Medallion Architecture)
