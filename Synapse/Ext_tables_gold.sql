create DATABASE scoped CREDENTIAL cred_nani
WITH
    IDENTITY = 'Managed Identity'


create EXTERNAL DATA SOURCE silver_ext
WITH
(
    LOCATION = 'https://projectdatalakeacc.dfs.core.windows.net/silver',
    CREDENTIAL = cred_nani
)


create EXTERNAL DATA SOURCE gold_ext
WITH
(
    LOCATION = 'https://projectdatalakeacc.dfs.core.windows.net/gold',
    CREDENTIAL = cred_nani
)


create EXTERNAL FILE FORMAT format_parquet1
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)


----------------------------------------
--CREATE EXT TABLES Calender
----------------------------------------
create EXTERNAL TABLE gold1.extcalender
WITH
(
    LOCATION = 'extcalender',
    DATA_SOURCE = gold_ext,
    FILE_FORMAT = format_parquet1
) as 
SELECT * from gold1.calender;

----------------------------------------
--CREATE EXT TABLES Customers
----------------------------------------
create EXTERNAL TABLE gold1.extcustomers
WITH
(
    LOCATION = 'extcustomers',
    DATA_SOURCE = gold_ext,
    FILE_FORMAT = format_parquet1
) as 
SELECT * from gold1.customers;

----------------------------------------
--CREATE EXT TABLES Product_Categories
----------------------------------------
create EXTERNAL TABLE gold1.extProduct_Categories
WITH
(
    LOCATION = 'extProduct_Categories',
    DATA_SOURCE = gold_ext,
    FILE_FORMAT = format_parquet1
) as 
SELECT * from gold1.Product_Categories;

----------------------------------------
--CREATE EXT TABLES Product_Subcategories
----------------------------------------
create EXTERNAL TABLE gold1.extProduct_Subcategories
WITH
(
    LOCATION = 'extProduct_Subcategories',
    DATA_SOURCE = gold_ext,
    FILE_FORMAT = format_parquet1
) as 
SELECT * from gold1.Product_Subcategories;

----------------------------------------
--CREATE EXT TABLES Products
----------------------------------------
create EXTERNAL TABLE gold1.extProducts
WITH
(
    LOCATION = 'extProducts',
    DATA_SOURCE = gold_ext,
    FILE_FORMAT = format_parquet1
) as 
SELECT * from gold1.Products;

----------------------------------------
--CREATE EXT TABLES Returns
----------------------------------------
create EXTERNAL TABLE gold1.extReturns
WITH
(
    LOCATION = 'extReturns',
    DATA_SOURCE = gold_ext,
    FILE_FORMAT = format_parquet1
) as 
SELECT * from gold1.returns;

----------------------------------------
--CREATE EXT TABLES Sales
----------------------------------------
create EXTERNAL TABLE gold1.extsales
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = gold_ext,
    FILE_FORMAT = format_parquet1
) as 
SELECT * from gold1.sales;


--------------------------------------
--CREATE EXT TABLES Territories
----------------------------------------
create EXTERNAL TABLE gold1.extTerritories
WITH
(
    LOCATION = 'extTerritories',
    DATA_SOURCE = gold_ext,
    FILE_FORMAT = format_parquet1
) as 
SELECT * from gold1.Territories;


