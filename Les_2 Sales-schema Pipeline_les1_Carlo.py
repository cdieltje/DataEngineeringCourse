# Databricks notebook source
# Hoe moet ik met code verbinden met mijn SQL Server? 
# jdbc URL + ConnectionProperties

#-------SOLUTION---------
# make connection string WideWorldImportersOLTP in Azure: https://portal.azure.com/#@bmatix.be/resource/subscriptions/43682ca1-5d07-442a-a331-69f8abdac3e4/resourceGroups/DataEngineeringCourse/providers/Microsoft.Sql/servers/world-wide-importers/databases/WideWorldImportersOLTP/overview > settings > connection strings > JDBC
# get pwd, username, driver (= name of database) from Excel and put in a var

# pwd = '^9i+2C[^ns1;'
# username = 'test_user_pls_ignore'

pwd = 'Carlo'
username = 'A8k#Zp!fX2@bQ9'

# pwd = 'Els'
# username = 'Q7!mDx#3Zp$2hV'

connection_string = f"jdbc:sqlserver://world-wide-importers.database.windows.net:1433;database=WideWorldImportersOLTP;user={username};password={pwd};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# print('Test connection string with credentials: ', connection_string)

# put name of database (driver) in a var
driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'

# COMMAND ----------

# Test query opstellen om te zien of verbinding geslaagd is
# Alias geven aan elke query!

#----------SOLUTION----------------

# Make df via jdbc. jdbc connection needs following params: url, properties, and choose table
connection_properties = {'user': 'test_user_pls_ignore', 'password': '^9i+2C[^ns1;', 'driver': driver}
df_sales = spark.read.jdbc(url=connection_string, table='Sales.Orders', properties=connection_properties)

# print('Display df:')
# df_sales.display()

test_query = '(SELECT TOP 3 * FROM Sales.Orders) AS alias' # every query needs an alias in jdbc
# test_query = '(SELECT 1 AS column_name) AS alias' 

try:
    spark.read.jdbc(url=connection_string, table=test_query, properties=connection_properties)
    print('Connection succeeded')
except:
    print('Connection error')


# COMMAND ----------

# Welk schema willen wij uitlezen?
# Hoe laden wij het resultaat van een query tegen de database in een dataframe? 
# Inspecteer het dataframe

#----------SOLUTION----------------

# work with vars for clarity during dev => parameterize a lot
schema_to_read = 'Sales'

# Read Information schema from table: contains amongst others all table names. standardised schema
information_schema_query = f"(SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_to_read}') AS alias" # mind: combination double & single quotes

information_schema_df = spark.read.jdbc(url=connection_string, table=information_schema_query,properties=connection_properties)
information_schema_df.display()


# df_test = spark.read.jdbc(url=connection_string, table=test_query, properties=connection_properties)
# df_test.display()



# COMMAND ----------

# Data Lakehouse, what's in a name?
# Schema creeëren (eerst droppen :))
# For-loop over dataframe om een lijst met tabelnamen te krijgen

#---------------SOLUTION---------------
# Whats's in name? Data Lakehouse: 'Data Lake' to store files, 'house' possibility to write sql-queries against

# Create Schema (The schema defines how data is organized within a (relational) database, with table names etc.)
# Drop schema 'sales' (just for sake of the course in order to start from clean slate)
# How to drop: See databricks > SQL Editor > hive_metastore > sales => DROP SCHEMA IF EXISTS Sales CASCADE;

# Create schema (see code in Excel)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_to_read}") # sql query therefore spark.sql => after running schema 'sales is added in databricks > SQL Editor > hive_metastore

# Create list of table names
table_lst = [] # start with creating empty list

for row in information_schema_df.collect(): # collect = get the actual values of a df
    table_lst.append(row["table_name"])

print(table_lst)

# COMMAND ----------

# Voor elke tabelnaam in ons lijstje gaan we de data inladen, wegschrijven als delta table en een tabel in ons data warehouse registreren
# Locatie bepalen in data lake voor onze tabellen

#----------SOLUTION----------------
schema_to_read = 'sales'
# Determine location for our tables
delta_base_path = "dbfs:/delta/{schema_to_read}/" # copied from Excel, this part means: 1/ look into the filesystem of your Databricks workspace ('dbfs' = database file system), comparable with the C: drive on your computer. 2/ delta = delta folder for delta tables, just a convention btw to let everybody know about the file format 'delta table'. You can call the folder anything you want, but that's not ideal) 3/ we use var schema_to_read

# Create table in data warehouse for every table of our database. This is dynamic, if a new table is added to the database, it's added to the list and it will be created in the warehouse.
for table_name in table_lst:
    table_path = f"{delta_base_path}{table_name}" # each table gets its own folder
    # table_name_path = delta_base_path + table_name # each table gets its own folder

    # test if table already exists with try/except: in try is the part when table exists: do nothing, in except we have to create the new table
    try: # if table exists, don't add it
        spark.read.format("delta").load("table_path") # code from Excel, We will read a delta table from the location defined in the table_name. Not jdbc as before, but delta format. 
        print("table already exists. We skip it")
    except: # if table does not exist, make df via jdbc, make delta file from that df, store delta file in the lake
        print("the table does not yet exists. Proceeding with data extraction")
        query = f"(SELECT * FROM {schema_to_read}.{table_name}) AS query"
        df = spark.read.jdbc(url=connection_string, table=query, properties=connection_properties)
        df.write.format("delta").mode("overwrite").save(table_path) # df needs to be written as a delta table file in our datalake. Mode 'overwrite': to make it easy for now, later on, we're going to append etc. Save: location where to save (code from Excel)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {schema_to_read}.{table_name} USING DELTA LOCATION '{table_path}'") # register delta tables in lake.Part 'IF NOT EXISTS' is not needed in this case, this has been checked already above. 

# If run successfully, you can check the result in the lake. first go to SQL Databricks > SQL Warehouses > run 'Serverless Starter Warehouse', then check the tables in Databricks > SQL Editor > Hive_metastore' => SELECT * from sales.orders

# COMMAND ----------

# MAGIC %md
# MAGIC **Summary**
# MAGIC
# MAGIC Data engineering in a lot of cases is running sql in a structured way.
# MAGIC
# MAGIC This was a simple pipeline which checks everything and reads it. Next lesson: for bigger data volumes we need more to make it production read, amongst others:
# MAGIC 1/ incremental load
# MAGIC 2/ logging of errors (tolerance) etc.
# MAGIC 3/ making pipelines recurring 

# COMMAND ----------


