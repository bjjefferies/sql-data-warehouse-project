/*
=================================================================
Stored Procedure: Bronze Layer Load
=================================================================
Purpose:
	This stored procedure loads data into bronze schema from csv
	files.
	It performs the following actions:
		- truncates the bronze tables before loading
		- uses BULK INSERT to load data from CSV's to bronze tables

-- This procedure can be run each day to upload new data from the sources
-- to the data warehouse.  Bulk insert of bronze layer data from csv.
-- This script is converted to a stored procedure to be automated daily

Parameters: None

Usage Example: EXEC bronze.load_bronze
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN
	
	-- declare start/end time vars to track total ETL time
	DECLARE @etl_start DATETIME, @etl_end DATETIME;
	DECLARE @start_time DATETIME, @end_time DATETIME;
	
	BEGIN TRY   
	
		PRINT '=============================================';
		PRINT 'Loading the bronze layer';
		PRINT '=============================================';

		PRINT '---------------------------------------------';
		PRINT 'Loading crm tables';
		PRINT '---------------------------------------------';

		SET @etl_start = GETDATE();

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info; -- clears all values

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\bjjef\OneDrive\Documents\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'; 
		PRINT '>> -------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info; -- clears all values

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\bjjef\OneDrive\Documents\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'; 
		PRINT '>> -------------------------------';



		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details; -- clears all values

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\bjjef\OneDrive\Documents\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'; 
		PRINT '>> -------------------------------';



		PRINT '---------------------------------------------';
		PRINT 'Loading erp tables';
		PRINT '---------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12; -- clears all values

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\bjjef\OneDrive\Documents\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'; 
		PRINT '>> -------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101; -- clears all values

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\bjjef\OneDrive\Documents\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.'; 
		PRINT '>> -------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2; -- clears all values

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\bjjef\OneDrive\Documents\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST( DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds.';
		PRINT '>> -------------------------------';

		SET @etl_end = GETDATE();
		PRINT '============================================================';
		PRINT 'Batch Load of Bronze Layer Completed';
		PRINT '>> ETL Load Duration: ' + CAST( DATEDIFF(second, @etl_start ,@etl_end) AS NVARCHAR) + ' seconds.';
		PRINT '============================================================';

	END TRY
	
	BEGIN CATCH -- executes if error occurs in try
		PRINT '==================================================';
		PRINT 'ERROR OCCURRED DURING LOAD OF BRONZE LAYER';
		PRINT 'ERROR MESSAGE:' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER:' + CAST( ERROR_NUMBER() AS NVARCHAR);
		PRINT '==================================================';
	END CATCH

END

