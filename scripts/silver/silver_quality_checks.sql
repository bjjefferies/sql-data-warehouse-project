
USE DataWarehouse;

-- quality checks script for checking bronze layer data
-- most commands have been changed to check silver. layer after ETL
-- to re-run checks of bronze layer, change back to bronze.

SELECT 
	prd_id,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1

--check for duplicates and null cst_id
-- Expectation: no results means no duplicates
SELECT
	sls_ord_num,
	COUNT(*)
FROM silver.crm_sales_details
GROUP BY sls_ord_num
HAVING COUNT(*) > 1 OR sls_ord_num IS NULL
ORDER BY COUNT(*) DESC


-- checks multiple orders to investigate
-- represents one order of multiple products, all dts/cust align
-- this test shows the data is ok, we have one row for each product in the order
SELECT * FROM silver.crm_sales_details
WHERE sls_ord_num IN (SELECT
							sls_ord_num
						FROM silver.crm_sales_details
						GROUP BY sls_ord_num
						HAVING COUNT(*) > 7 OR sls_ord_num IS NULL
						)


-- check for sls_prd_key not in crm.prd.info
-- it would be ok for not all prd_info products to have never been sold
-- however, we would not want to be selling products not in our inventory
SELECT *
FROM bronze.crm_sales_details 
WHERE sls_prd_key NOT IN (	SELECT DISTINCT prd_key
						FROM silver.crm_prd_info)

-- check crm_sales_details for 

SELECT *
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT DISTINCT prd_key
							FROM silver.cst_id)

-- ETL exploration for dates in bronze.crm_sales_details
-- dates stored in bronze are sequence of digits
-- can't cast integers that are negative or 0 to date
-- must also be of length 8 (yyyymmdd)

SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8

SELECT sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR
LEN(sls_due_dt) != 8 OR
sls_due_dt < 19000101 OR
sls_due_dt > 20500101


-- check dates in crm_sales_details, order must be before ship/due
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
OR sls_order_dt > sls_due_dt


-- check data qual for sales/quantity/price
-- cannot have 0 or negative price
-- sales = price * quantity
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
OR sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL



SELECT 
	sls_sales as old_sales,
	sls_quantity as old_quantity,
	sls_price as old_price,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END as sls_sales,
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
	END as sls_price
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL OR sls_price <= 0



-- CHECK FOR UNWANTED SPACES
-- expectation: no results
SELECT sls_ord_num, TRIM(sls_ord_num) AS trimmed
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)


-- check for nulls or negative numbers
-- applies to product cost
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL


-- Data Standardization and Consistency

SELECT DISTINCT prd_line
FROM silver.crm_prd_info
WHERE prd_line != TRIM(prd_line)


-- check invalid order dates
-- prd_start is after prd_end
SELECT TOP(100) *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt


SELECT *,
	LEAD(prd_start_dt, 1, 0) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 as 'new_end'
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')




-- Check for Nulls or duplicates in crm_cust_info
-- Expectation: No results

SELECT
	cst_id,
	COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL
ORDER BY COUNT(*) DESC




-- Check for unwanted spaces
-- Expectation: No Results
SELECT 
	cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Found: 15 first names with leading/trailing spaces







-- ERP tables data checks

-- erp.cust_az12
-- check quality of cid (customer id column) to see if it matches crm_cust_info
SELECT * FROM silver.erp_cust_az12
WHERE cid LIKE '%AW00011197%'


-- transform and check that all cid start with 'AW'
SELECT 
	cid as old_cid,
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- check if cid starts with NAS
		ELSE cid
	END AS cid
FROM silver.erp_cust_az12
WHERE CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- check if cid starts with NAS
		ELSE cid END
	NOT LIKE 'AW%'


-- CHECK if new cid aren't present in crm cust_key
SELECT 
	cid as old_cid,
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- check if cid starts with NAS
		ELSE cid
	END AS cid
FROM silver.erp_cust_az12
WHERE CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- check if cid starts with NAS
		ELSE cid END
	NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)


-- check quality of birthdate column, is date in the future?

SELECT bdate FROM silver.erp_cust_az12
WHERE bdate > GETDATE()


-- check qual and consistency for gender date

SELECT DISTINCT gen FROM silver.erp_cust_az12

-- CHECK SOLUTION
SELECT DISTINCT
	gen as old_gen,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		ELSE 'n/a'
	END AS gen
FROM silver.erp_cust_az12




-- erp_loc_a101 quality checks

-- CHECK FORMATTING OF primary key cid
-- bronze layer has - between AW and digits
SELECT cid
FROM silver.erp_loc_a101
WHERE cid NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

-- confirm that replace fix works
SELECT 
	cid AS old_cid,
	REPLACE(cid, '-', '') AS cid
FROM silver.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)


-- Country column consistency and normalization
-- low cardinality data, needs to be manually checked

SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry

SELECT DISTINCT
	cntry as old_cntry,
	CASE 
		WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
		WHEN UPPER(TRIM(cntry)) IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
		WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101
