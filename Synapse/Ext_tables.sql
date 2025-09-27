create DATABASE scoped CREDENTIAL cred_mani
WITH
    IDENTITY = 'Managed Identity'


create EXTERNAL DATA SOURCE source_ext
WITH
(
    LOCATION = 'https://projectdatalakeacc.dfs.core.windows.net/transformation',
    CREDENTIAL = cred_mani
)


create EXTERNAL DATA SOURCE sink_ext
WITH
(
    LOCATION = 'https://projectdatalakeacc.dfs.core.windows.net/sink',
    CREDENTIAL = cred_mani
)


create EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)


----------------------------------------
--CREATE EXT TABLES Calender
----------------------------------------
create EXTERNAL TABLE gold.calenderext
WITH
(
    LOCATION = 'extcalender',
    DATA_SOURCE = sink_ext,
    FILE_FORMAT = format_parquet
) as 
SELECT * from gold.calender;

----------------------------------------
--CREATE EXT TABLES Customers
----------------------------------------
create EXTERNAL TABLE gold.extcustomers
WITH
(
    LOCATION = 'extcustomers',
    DATA_SOURCE = sink_ext,
    FILE_FORMAT = format_parquet
) as 
SELECT * from gold.customers;

----------------------------------------
--CREATE EXT TABLES Product_Categories
----------------------------------------
create EXTERNAL TABLE gold.extProduct_Categories
WITH
(
    LOCATION = 'extProduct_Categories',
    DATA_SOURCE = sink_ext,
    FILE_FORMAT = format_parquet
) as 
SELECT * from gold.Product_Categories;

----------------------------------------
--CREATE EXT TABLES Product_Subcategories
----------------------------------------
create EXTERNAL TABLE gold.extProduct_Subcategories
WITH
(
    LOCATION = 'extProduct_Subcategories',
    DATA_SOURCE = sink_ext,
    FILE_FORMAT = format_parquet
) as 
SELECT * from gold.Product_Subcategories;

----------------------------------------
--CREATE EXT TABLES Products
----------------------------------------
create EXTERNAL TABLE gold.extProducts
WITH
(
    LOCATION = 'extProducts',
    DATA_SOURCE = sink_ext,
    FILE_FORMAT = format_parquet
) as 
SELECT * from gold.Products;

----------------------------------------
--CREATE EXT TABLES Returns
----------------------------------------
create EXTERNAL TABLE gold.extReturns
WITH
(
    LOCATION = 'extReturns',
    DATA_SOURCE = sink_ext,
    FILE_FORMAT = format_parquet
) as 
SELECT * from gold.returns;

----------------------------------------
--CREATE EXT TABLES Sales
----------------------------------------
create EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION = 'extsales',
    DATA_SOURCE = sink_ext,
    FILE_FORMAT = format_parquet
) as 
SELECT * from gold.sales;


--------------------------------------
--CREATE EXT TABLES Territories
----------------------------------------
create EXTERNAL TABLE gold.extTerritories
WITH
(
    LOCATION = 'extTerritories',
    DATA_SOURCE = sink_ext,
    FILE_FORMAT = format_parquet
) as 
SELECT * from gold.Territories;


