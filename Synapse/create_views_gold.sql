--------------------------------------
-- created View Calender
--------------------------------------

create view gold1.calender as
select * from 
OPENROWSET(
    BULK 'https://projectdatalakeacc.dfs.core.windows.net/silver/Calender/',
    FORMAT = 'PARQUET'
) as querycal;


--------------------------------------
-- created View Customers
--------------------------------------

create view gold1.customers as
select * from 
OPENROWSET(
    BULK 'https://projectdatalakeacc.dfs.core.windows.net/silver/Customers/',
    FORMAT = 'PARQUET'
) as queryCus;


--------------------------------------
-- created View Product_Categories
--------------------------------------

create view gold1.Product_categories as
select * from 
OPENROWSET(
    BULK 'https://projectdatalakeacc.dfs.core.windows.net/silver/Product_Categories/',
    FORMAT = 'PARQUET'
) as queryprocat;

--------------------------------------
-- created View Product_Subcategories
--------------------------------------

create view gold1.product_subcategories as
select * from 
OPENROWSET(
    BULK 'https://projectdatalakeacc.dfs.core.windows.net/silver/Product_Subcategories/',
    FORMAT = 'PARQUET'
) as queryprosub;

--------------------------------------
-- created View Products
--------------------------------------

create view gold1.products as
select * from 
OPENROWSET(
    BULK 'https://projectdatalakeacc.dfs.core.windows.net/silver/Products/',
    FORMAT = 'PARQUET'
) as querypro;

--------------------------------------
-- created View Returns
--------------------------------------

create view gold1.returns as
select * from 
OPENROWSET(
    BULK 'https://projectdatalakeacc.dfs.core.windows.net/silver/Returns/',
    FORMAT = 'PARQUET'
) as queryret;

--------------------------------------
-- created View Sales
--------------------------------------

create view gold1.sales as
select * from 
OPENROWSET(
    BULK 'https://projectdatalakeacc.dfs.core.windows.net/silver/Sales/',
    FORMAT = 'PARQUET'
) as querysal;

--------------------------------------
-- created View Territories
--------------------------------------

create view gold1.territories as
select * from 
OPENROWSET(
    BULK 'https://projectdatalakeacc.dfs.core.windows.net/silver/Territories/',
    FORMAT = 'PARQUET'
) as querytet;


