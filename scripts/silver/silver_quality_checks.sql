
-- quality checks script for checking bronze layer data


--check for duplicates and null cst_id
-- Expectation: no results means no duplicates
SELECT
	prd_id,
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


-- CHECK FOR UNWANTED SPACES
-- expectation: no results
SELECT prd_nm, TRIM(prd_nm) AS trimmed
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)


-- check for nulls or negative numbers
-- applies to product cost
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL


-- Data Standardization and Consistency

SELECT DISTINCT prd_line
FROM bronze.crm_prd_info
WHERE prd_line != TRIM(prd_line)


-- check invalid order dates
-- prd_start is after prd_end
SELECT TOP(100) *
FROM bronze.crm_prd_info
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