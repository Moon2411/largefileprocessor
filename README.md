
# LargeFileProcessor

**** Version 1.0***

Aim is to build a system which is able to handle long running processes in a distributed fashion.
Problem statement
We need to be able to import products from a CSV file and into a database. There are half a million product details to be imported into the database. 
After the import, we will run an aggregate query to give us no. of products with the same name.

Points to achieve
1. Code should follow concept of OOPS
2. Code should support for regular non-blocking parallel ingestion of the given file into a table. Consider thinking about the scale of what should happen if the file is to be processed in 2 mins.
3. Code should support for updating existing products in the table based on `sku` as the primary key. (Yes, we know about the kind of data in the file. You need to find a workaround for it)
4. All product details are to be ingested into a single table
5. An aggregated table on above rows with `name` and `no. of products` as the columns

Programming Language used: Python
Database used: Sqlite

Assumptions:
1. No History data is retained i.e whenever a new name or description is found for the same sku we update the data with changes.
2. Last record that comes in the csv file will be retained as there is no timestamp to consider lastest data
3. Everytime aggregate_job is run a new product_agg_{timestamp} with the latest timestamp
4. No History data is retained for product_agg_{timestamp} table

Deliverables:
a. Steps to run your code. As less steps we are to run, better for you (Hint: Docker)

Windows:
1. Put the input csv file with name "products.csv" into data folder
2. Run the bat file as .\src\LargeFileProcessingJob\run.bat
Note: There are 2 modes: Full load and Incremental load
Full load should be used whenever you want to load the data for the first time
Incremental load should be used whenever there is a new file with updates
In the bat file we have first run the full load and then incremental load but if only incremental is required we can remove full load from the bat file and then run the incremental load

Linux:
1. Put the input csv file with name "products.csv" into data folder
2. Run the sh file as .\src\LargeFileProcessingJob\run.sh
Note: There are 2 modes: Full load and Incremental load
Full load should be used whenever you want to load the data for the first time
Incremental load should be used whenever there is a new file with updates
In the sh file first run the full load runs and then incremental load but if only incremental is required we can remove full load from the sh file and then run the incremental load

b. Details of all the tables and their schema, [with commands to recreate them]
Database Name: dummy
Tables Name:
products-Name,sku,decription
product_agg_{timestamp}-name,no_products where timestamp changes as per new table that is generated when the aggregate_job is run

Both the tables are created in the database if they do not exist with the code itself so no need to run the below queries separately
Products query: """Create Table IF NOT EXISTS products(
                name nvarchar(50),
                sku nvarchar(50) PRIMARY KEY,
                description TEXT)"""

timestamp = datetime.strftime(datetime.now(), format='%Y%m%d_%H%M%S')
Product_agg: f"""
        create table product_agg_{timestamp}(
        name nvarchar(50) PRIMARY KEY,
        no_products INT);
        """

c. What is done from “Points to achieve” and number of entries in all your tables with sample 10 rows from each
All the points from "Points to achieve" are done.
No of entries in Products: 466693
No of entries in Products_agg: 

d. What is not done from “Points to achieve”. If not achieved, write the possible reasons and current workarounds.
All the points from "Points to achieve" are done.

e. What would you improve if given more days
Would have tried to maintain history data with some workaround


