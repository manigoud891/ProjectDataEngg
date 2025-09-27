--------------------------------------
-- created View Calender
--------------------------------------

create view gold.calender as
select * from 
OPENROWSET(
    BULK 'https://projectdatalakeacc.dfs.core.windows.net/transformation/Calender/',
    FORMAT = 'PARQUET'
) as querycal;


--------------------------------------
-- created View Customers
--------------------------------------

create view gold.customers as
select * from 
OPENROWSET(
    BULK 'https://projectdatalakeacc.dfs.core.windows.net/transformation/Customers/',
    FORMAT = 'PARQUET'
) as queryCus;


--------------------------------------
-- created View Product_Categories
--------------------------------------

create view gold.Product_categories as
select * from 
OPENROWSET(
    BULK 'https://projectdatalakeacc.dfs.core.windows.net/transformation/Product_Categories/',
    FORMAT = 'PARQUET'
) as queryprocat;

--------------------------------------
-- created View Product_Subcategories
--------------------------------------

create view gold.product_subcategories as
select * from 
OPENROWSET(
    BULK 'https://projectdatalakeacc.dfs.core.windows.net/transformation/Product_Subcategories/',
    FORMAT = 'PARQUET'
) as queryprosub;

--------------------------------------
-- created View Products
--------------------------------------

create view gold.products as
select * from 
OPENROWSET(
    BULK 'https://projectdatalakeacc.dfs.core.windows.net/transformation/Products/',
    FORMAT = 'PARQUET'
) as querypro;

--------------------------------------
-- created View Returns
--------------------------------------

create view gold.returns as
select * from 
OPENROWSET(
    BULK 'https://projectdatalakeacc.dfs.core.windows.net/transformation/Returns/',
    FORMAT = 'PARQUET'
) as queryret;

--------------------------------------
-- created View Sales
--------------------------------------

create view gold.sales as
select * from 
OPENROWSET(
    BULK 'https://projectdatalakeacc.dfs.core.windows.net/transformation/Sales/',
    FORMAT = 'PARQUET'
) as querysal;

--------------------------------------
-- created View Territories
--------------------------------------

create view gold.territories as
select * from 
OPENROWSET(
    BULK 'https://projectdatalakeacc.dfs.core.windows.net/transformation/Territories/',
    FORMAT = 'PARQUET'
) as querytet;


