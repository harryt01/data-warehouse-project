/*
==========
Quality checks
==========
Script Purpose:
    Performs various quality checks for data consistency, accuracy, 
    and standardization across the Silver layer, including:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

==========
*/

SELECT * FROM bronze.crm_cust_info;

--check for duplicated or null values
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- ==> query choosing the latest to-date from the duplicated rows
SELECT *
FROM (
	SELECT 
		*, 
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	--WHERE cst_id = 29466
	)a
WHERE flag_last = 1;

--check for unwanted spaces
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- ==> query trimming the spaces
SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	cst_marital_status,
	cst_gender,
	cst_create_date
FROM (
	SELECT 
		*, 
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	--WHERE cst_id = 29466
	)a
WHERE flag_last = 1;

--check valid limit-value data
SELECT DISTINCT cst_gender
FROM bronze.crm_cust_info;
--found null and abbreviated values

--check for duplicated or null values
SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

--check for unwanted spaces
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

--check valid limit-value data
SELECT DISTINCT gen
FROM bronze.erp_cust_az12;

--check for nulls or negatives
SELECT prd_cost 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

--check for invalid date orders (consistency) - data standardization
SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
	OR sls_ship_dt > sls_due_dt;

--check for invalid dates
SELECT 
	NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
	OR LEN(sls_order_dt) != 8
	OR sls_order_dt NOT BETWEEN '19000101' AND '20300101';

SELECT 
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt OR sls_ship_dt > sls_due_dt;

--check data consistency: between sales, quantity & price
/*rules:
	if sales is negative/zero/null/not quantity * |price|, derive it using price & quantity
	if price is zero/null, derive it using sales and quantity
	if price is negative, convert to the absolute positive val
*/
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales,	sls_quantity, sls_price;

--crm_prd_info
SELECT 
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_dt DATETIME2 DEFAULT GETDATE()
);

--

--erp_cust_az12


SELECT
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END	cid,
	CASE
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END bdate,
	CASE 
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END gen
FROM bronze.erp_cust_az12;

/*WHERE 
	CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END 
	NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info);*/

--identify out-of-range dates
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1925-01-01' OR bdate > GETDATE();

--silver.erp_loc_a101
SELECT 
	REPLACE(cid, '-', '') cid,
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END cntry
FROM bronze.erp_loc_a101
WHERE REPLACE(cid, '-', '') NOT IN 
(SELECT cst_key FROM silver.crm_cust_info);

--data standardization & consistency
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;

--cntry column
SELECT DISTINCT 
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;

--silver.erp_px_cat_g1v2
SELECT 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2;

--check for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat <> TRIM(cat) 
	OR subcat != TRIM(subcat) 
	OR maintenance <> TRIM(maintenance);

--data standardization
SELECT DISTINCT subcat
FROM bronze.erp_px_cat_g1v2;

SELECT * FROM bronze.erp_px_cat_g1v2