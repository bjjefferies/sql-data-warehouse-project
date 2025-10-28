/*
========================================================================
DDL Script: Gold Layer Views
========================================================================
Script Purpose:
This script creates the three views that comprise the gold layer of the database.
The gold layers is the final dimension with a fact and two dim tables in a star schema.

Each view performs transformations on and combines data from silver layer tables. The
gold layer is business ready for all business end users.

Usage:
These views can be queried directly for analytics and reporting.

*/


USE DataWarehouse;

-- Create gold layer customer dimension table with all availabe descriptors of cust.

CREATE VIEW gold.dim_customer AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	loc.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE 
		WHEN ci.cst_gndr = 'n/a' AND bd.gen != 'n/a' THEN bd.gen
		ELSE ci.cst_gndr
	END AS gender, -- integrates multiple gender sources of gender data
	bd.bdate AS birth_date,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 bd
ON ci.cst_key = bd.cid
LEFT JOIN silver.erp_loc_a101 loc
ON ci.cst_key = loc.cid





-- create product dimension table

CREATE VIEW gold.dim_product AS
SELECT
	ROW_NUMBER() OVER(ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key,
	pi.prd_id AS product_id,
	pi.prd_key AS product_number,
	pi.prd_nm AS product_name,
	pi.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance AS maintenance,
	pi.prd_cost AS cost,
	pi.prd_line AS product_line,
	pi.prd_start_dt AS start_date
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pc.id = pi.cat_id
WHERE pi.prd_end_dt IS NULL -- filters out non-current data




-- Create Fact Sales Table

CREATE VIEW gold.fact_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	pd.product_key AS product_key,
	cd.customer_key AS customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS item_price
FROM silver.crm_sales_details sd
LEFT JOIN  gold.dim_product pd
ON sd.sls_prd_key = pd.product_number
LEFT JOIN gold.dim_customer cd
ON sd.sls_cust_id = cd.customer_id




