
/*
--------------------------------
Create Databse and Schema
---------------------------------
Script propose:
This script creates a new database named 'DataWarehouse' after checking if it already exists. 
if the database exist, it is dropped and recreated. Additionally, the scripts sets up three schemas withing the database: 'Bronze', 'Silver', 'Gold'

Warning:
Running this script will drop the entire 'DataWarehouse' databse if it exists.
All data in the databse will be permanently deleted. Proceed with caution and ensure you have proper backups before running hte scripts.
*/

use master;
Go
----Drop and recreate the DataWarehouse databse
if exists (select 1 from sys.databases where name = 'DataWarehouse')
BEGIN
	ALTER Database DataWarehouse SET SINGLE_USER with ROLLBACK IMMEDIATE;
	DROP Database DataWarehouse;
END;
GO

------Creating the Database and name as DataWarehouse
create Database DataWarehouse; 

use DataWarehouse;

--Creating a Schemas (Bronze, Silver, Gold)Based on architecture style.
Create schema bronze;
go

Create schema silver;
go

Create schema gold;
go

---------------------------------------------Customer Table  1 
-----Adding TSQL(Control flow logic) 
if OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
----Creating customer information table
CREATE TABLE bronze.crm_cust_info(
	cst_id				INT,
	cst_key				NVARCHAR(50),
	cst_firstname		NVARCHAR(50),
	cst_lastname		NVARCHAR(50),
	cst_material_status NVARCHAR(50),
	cst_gndr			NVARCHAR(50),
	cst_create_date		DATE
); 

-----------------------------------------------Prod Table  2 
-----Adding TSQL(Control flow logic) 
if OBJECT_ID ('bronze.crm_prod_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prod_info;
----Creating customer prod info TABLE
CREATE TABLE bronze.crm_prod_info(
	prd_id			INT,
	prd_key			NVARCHAR(50),
	prd_nm			NVARCHAR(50),
	prd_cost		INT,
	prd_line		NVARCHAR(50),
	prd_start_dt	DATETIME,
	prd_end_dt		DATETIME
);


-----------------------------------------SALES Table 3
-----Adding TSQL(Control flow logic) 
if OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
----Creating SALES DETAILS TABLE
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num			NVARCHAR(50),
	sls_prd_key			NVARCHAR(50),
	sls_cust_id			INT,
	sls_order_dt		INT,
	sls_ship_dt			INT,
	sls_due_dt			INT,
	sls_sales			INT,
	sls_quantity		INT,
	sls_price			INT
);


--------ERP LOGIC A101 Table
-----Adding TSQL(Control flow logic) 
if OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
	cid		NVARCHAR(50),
	cntry	NVARCHAR(50)
);


---------erp_cust_az12 Table
-----Adding TSQL(Control flow logic) 
if OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
	cid		NVARCHAR(50),
	bdate	DATE,
	gen		NVARCHAR(50)
);


---------erp_px_cat_g1v2 Table
-----Adding TSQL(Control flow logic) 
if OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
	id			NVARCHAR(50),
	cat			NVARCHAR(50),
	subcat		NVARCHAR(50),
	maintenance NVARCHAR(50)
);

EXEC bronze.load_bronze 
----------------------------Bulk insert for customer info------------------------------------------------------------
Create or alter procedure bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		 
		SET @batch_start_time = GETDATE();
		PRINT '===============================================================';
		PRINT 'loading the bronze layer';
		PRINT '===============================================================';

		PRINT '-----------------------------------------------------';
		PRINT'LOADING CRM Tables';
		PRINT '-----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm.cust.info'
		TRUNCATE TABLE bronze.crm_cust_info;
		------------BULK INSERT------

		PRINT '>>Inserting Data into Table: bronze.crm.cust.info'
		BULK INSERT bronze.crm_cust_info
		from 'C:\Users\lovel\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		------------------------------------------BULK INSERT--table 2 prod----
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_prod_info';
		TRUNCATE TABLE bronze.crm_prod_info;

		PRINT '>>Inserting Data into Table: bronze.crm_prod_info';
		BULK INSERT bronze.crm_prod_info
		from 'C:\Users\lovel\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		select count(*) from bronze.crm_prod_info
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';

		------------------------------------------BULK Insert----table 3------------------
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details ;

		PRINT '>>Inserting Data into Table: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details 
		from 'C:\Users\lovel\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';

		------------------------------------------Bulk Insert TABLE 4-----------------

		PRINT '-----------------------------------------------------';
		PRINT'LOADING ERP Tables';
		PRINT '-----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>>Inserting Data into Table: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		from 'C:\Users\lovel\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		------------------------------------------Bulk Insert TABLE 5-----------------------------------
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>>Inserting Data into Table: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		from 'C:\Users\lovel\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		------------------------------------------Bulk Insert TABLE 6---------------------------------
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>>Inserting Data into Table: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		from 'C:\Users\lovel\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' SECONDS';
		
		SET @batch_end_time = GETDATE();
		PRINT'======================================='
		PRINT 'LOADING Bronze layer is completed';
		PRINT 'total load duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time,  @batch_end_time) AS NVARCHAR) + '  seconds';
		PRINT '=====================================';
		END TRY
		BEGIN CATCH
		PRINT'=======================================';
		PRINT'Error occured during loading bronze layer';
		PRINT'Error message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT'=======================================';
		END CATCH
End
----------------------------------------------------------------------Bulk insert into Bronze Completed-----------------------------
------------Create Stored Procedure------------

