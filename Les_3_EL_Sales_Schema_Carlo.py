# Databricks notebook source
# MAGIC %md #### Eerst moeten we Databricks vertellen met welke server we willen verbinden, en hoe

# COMMAND ----------

# MAGIC %md
# MAGIC # Eigen notities
# MAGIC - Vorig Les (2): Naïeve aanpak => hoe loopen over alle tabellen + deze volledig overschrijven. Kost veel aan cloudkosten
# MAGIC - Deze les: data incrementeel aanvullen
# MAGIC - Sommige tabellen zijn system-versioned: under the hood multiple versions, you can toggle versions of this table in SSMS (eg 'Sales.Customers (System-Versioned)' ) => we willen alles waar '(System-Versioned)' in staat weggooien zoniet worden deze allemaal ingelezen: 
# MAGIC
# MAGIC VB SQL: 
# MAGIC
# MAGIC information_schema_query = f"""
# MAGIC
# MAGIC (SELECT
# MAGIC   table_name
# MAGIC   
# MAGIC FROM
# MAGIC   information_schema_tables
# MAGIC WHERE
# MAGIC   table_schema = '{schema_to_read}'
# MAGIC   AND table_name NOT LIKE '%Archive%') AS query """
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC OF zelfde in Python: 
# MAGIC
# MAGIC for row in schema_tables_df.collect():
# MAGIC
# MAGIC     if 'archive' in row['table_name']:
# MAGIC
# MAGIC       pass
# MAGIC
# MAGIC     else:
# MAGIC
# MAGIC       table_list.append(row['table_name'])
# MAGIC
# MAGIC print(table_list)
# MAGIC
# MAGIC - Pieter: kies voor sql (meer volk kent dat + simpeler + zal minder compute gebruiken)
# MAGIC - Pieter: wat betekent """ """ => als je zelfde string wil doortrekken over meerdere lijnen. Handig om SQL query in notebook te formatten over meerdere lijnen. Je moet wel binnen dezelfde indentatie van Python blijven
# MAGIC - verder modulariteit: we gieten herbruikbare functies in aparte notebook die je in andere notebooks kan importen

# COMMAND ----------

# SQLConnectionUtils is andere notebook die zelfgeschreven functies bevat
%run "/Workspace/Shared/utils/SQLConnectionUtils" # %run => via % kan je aangeven wat in de cel moet gebeuren. Dit is CLI-taal (Command Line Interface - Databricks heeft ook CLI under the hood). %run betekent hier: run notebook die op locatie X staat.

# COMMAND ----------

# # As cell above does not work 'could not open file for safe execution', here's a copy of code

# # Definieer een functie om een JDBC connection String te genereren
# def get_sql_jdbc_string(server_name: str, database_name: str, user: str, password: str) -> str:
#     return f"jdbc:sqlserver://{server_name}:1433;database={database_name};user={user}@{server_name};password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"


# # Definieer een functie om de jdbc connection properties te generen:
# def get_sql_connection_properties(user: str, password: str) -> dict:
#     return {"user": user, "password": password, "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}

# COMMAND ----------

# MAGIC %md
# MAGIC Incrementeel laden => 4 stappen
# MAGIC - bepaald wanneer data laatst geladen is
# MAGIC - lees nieuwe data uit de bron
# MAGIC - schrijf nieuwe data weg in datawarehouse
# MAGIC - update metadata met de info over wanneer deze laatst geladen is

# COMMAND ----------


# Genereer de nodige variables om te verbinden met een SQL server:

ServerName = "world-wide-importers.database.windows.net"
DatabaseName = "WideWorldImportersOLTP"
UserName = "test_user_pls_ignore"
Password = "^9i+2C[^ns1;"

URL = get_sql_jdbc_string(ServerName, DatabaseName, UserName, Password) # get_sql_jdbc_string: function in selfmade SQLConnectionUtils
ConnectionProperties = get_sql_connection_properties(UserName, Password)# get_sql_jdbc_string: function in selfmade SQLConnectionUtils

print(URL)
print(ConnectionProperties)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Laat ons de gegevens van die server eens testen met een connection check

# COMMAND ----------

# Test query runnen om te zien of verbinding geslaagd is
# Alias geven aan elke query

test_query = "(SELECT 1 AS test_column) AS alias"

try:
    spark.read.jdbc(url=URL, table=test_query, properties=ConnectionProperties)
    print("Connection Succesful!")
except:
    print("Connection Failed!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Nu moeten we vertellen aan Databricks welke tables er in het Sales-schema zitten. Aangezien wij dat niet allemaal willen uittypen, en het kan zijn dat er tabellen toegevoegd worden in de toekomst, gaan we dit programmatisch doen

# COMMAND ----------

# Welk schema willen wij uitlezen?
# Hoe laden wij het resultaat van een query tegen de database in een dataframe? 
# Toon het dataframe

schema_to_read = 'Sales'

information_schema_query = f"""
(SELECT 
    table_name 
FROM 
    information_schema.tables 
WHERE 
    table_schema = '{schema_to_read}' 
    AND table_name NOT LIKE '%Archive%') AS query"""
schema_tables_df = spark.read.jdbc(url=URL, table=information_schema_query, properties=ConnectionProperties)
schema_tables_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Lakehouse: what's in a name?

# COMMAND ----------

# Data Lakahouse, what's in a name?
# Schema creeëren (eerst droppen :))
# Lijst met tabellen creeëren
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_to_read}")

table_list = []
for row in schema_tables_df.collect():
    if 'Archive' in row['table_name']:
        pass
    else:
        table_list.append(row['table_name'])

print(table_list)

# COMMAND ----------

delta_base_path = f"dbfs:/delta/{schema_to_read}/"

for table_name in table_list:
    delta_table_path = f"{delta_base_path}{table_name}"
    
    try:
        spark.read.format("delta").load(delta_table_path)
        print(f"Delta table for {schema_to_read}.{table_name} already exists. Skipping.")
    
    except:
        print(f"Delta table for {schema_to_read}.{table_name} does not exist. Proceeding with data extraction.")
        table_query = f"(SELECT * FROM {schema_to_read}.{table_name}) AS query"
        df = spark.read.jdbc(url=URL, table=table_query, properties=ConnectionProperties)
        df.write.format("delta").mode("overwrite").save(delta_table_path)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {schema_to_read}.{table_name} USING DELTA LOCATION '{delta_table_path}'")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Nu hebben we een pipeline die elke tabel in het Sales-schema volledig uitleest en wegschrijft. Handig voor een eerste keer, en ook handig om in je pipeline te houden moesten er tabellen worden toegevoegd. Maar wat met nieuwe rows in de brontabellen? 

# COMMAND ----------

# We hebben een tabel in ons data warehouse met metadata
# table_name, last_loaded & highest_update_timestamp 

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {schema_to_read}.EL_Data (
        table_name STRING,
        last_loaded TIMESTAMP,
        highest_update_timestamp TIMESTAMP
    )
    USING DELTA
""")

# COMMAND ----------

# Highest timestamp per tabel uitlezen uit EL_Data
# Indien ernog geen record is voor die tabelnaam, willen we alle data laden
# Lees de nieuwe data in uit SQL Server, gefilterd op timestamp records (belangrijk: sommige hebben geen LastUpdatedWhen-kolom!)
# Dynamisch primary key van tabel ophalen uit information_schema
# merge or update nieuwe data in target
# Nieuwe highest timestamp uitlezen en wegschrijven naar EL_Data


for table_name in table_list:
    delta_table_path = f"{delta_base_path}{table_name}" # locatie waar delta tables staan in Databricks file system

    get_highest_timestamp_query = f"SELECT * FROM {schema_to_read}.EL_Data WHERE table_name = '{table_name}'"
    highest_update_df = spark.sql(get_highest_timestamp_query) # result = df met één rij, die rij is highest timestamp

    # Indien er nog geen record is voor die tabelnaam, willen we alle data laden
    if highest_update_df.isEmpty():
        highest_update_timestamp = '1900-01-01 00:00:00'
    else:
        highest_update_timestamp = highest_update_df.first()['highest_update_timestamp'] # we nemen eerste rij uit df via .first (=pyspark), normaal gezien is er ook maar één rij

    
    get_new_data_query = f"(SELECT * FROM {schema_to_read}.{table_name} WHERE LastEditedWhen > '{highest_update_timestamp}' ) AS query" # lasteditedwhen moet hoger zijn dan highest update date => dit maakt het dynamisch
    get_full_data_query = f"(SELECT * FROM {schema_to_read}.{table_name} ) AS query" # query om alle dimensions op te roepen. zal  telkens terugkomen

    # sommige velden hebben geen lasteditedwhen, maar wel valid from en valid to , daarom dat we twee queries schrijven (in 'try' en 'except') en niet één. het is een quork in de data dat niet elke tabel een lasteditedwhen heeft

    try: 
        # Nieuwe records laden, in temp view steken voor MERGE statement. We steken resultaat van SQL-queries in df's bij elke loop
        data_df = spark.read.jdbc(url=URL, table=get_new_data_query, properties=ConnectionProperties)  # zoek lasteditedwhen en steek data in df, als je lasteditedwhen niet vindt, dan ga je naar except.
        display(data_df)
        data_df.createOrReplaceTempView("data_df") # createOrReplaceTempView: we steken temporary view in data warehouse. Dus voor duur van één loop halen we data op als ware het een view in het data warehouse dat we kunnen gebruiken. Dus we registreren een python object als een temporaty view zodat we er sql tegen kunnen schrijven. En we moeten SQL schrijven omdat we sebiet MERGE INTO gaan doen.

        # we halen primary key op, hebben we nodig bij MERGE INTO. je neemt target tabel en source tabel en je vergelijkt die op een bepaalde key. Als key gevonden is gaan we updaten, als niet gevonden is, dan gaan we inserten (zie MERGE INTO). Op basis van welk veld? => obv primary key. Hoe vinden we primary key op een dynamische manier? (suboptimaal: steek pk's in list manueel en loop erover.) beter: via information_schema kan je pk's halen uit tabel table_constraints (daar staan constraints is) en constraint_name (daar staan kolomnamen in). Resultaat van primary_key_query = one row, one column table die primary key bevat
        primary_key_query = f"""
                            (SELECT 
                                Usage.COLUMN_NAME 
                            FROM 
                                INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS Constraints
                            JOIN 
                                INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS Usage
                                ON Usage.CONSTRAINT_NAME = Constraints.CONSTRAINT_NAME
                            WHERE 
                                Usage.TABLE_NAME = '{table_name}'
                                AND Constraints.CONSTRAINT_TYPE = 'PRIMARY KEY') as query
                            """

        primary_key = spark.read.jdbc(url=URL, table=primary_key_query, properties=ConnectionProperties).first()[0] # we executen bovenstaande query in de gegeven database. '.first() = we nemen eerste rij (zelfs al is er altijd maar 1 rij). [0] = we nemen eerste kolom (zelfs al is er maar 1 kolom)
        
        # SQL 'MERGE INTO' = op basis van twee tabellen 'source' en 'target' meng je de ene tabel in de andere (is iets anders dan Joinen): updaten bij bestaande rijen (= 'upsert'), nieuwe creëeren bij nieuwe rijen ('insert')
        # 'using': geeft aan welke data / query je wil gebruiken om te mergen
        # 'ON': op welk veld ga je mergen=> bij match: update. bij niet-match: insert
        merge_new_data_query = f"""MERGE INTO {schema_to_read}.{table_name} AS target
                                USING data_df AS source
                                ON target.{primary_key} = source.{primary_key}
                                WHEN MATCHED THEN
                                    UPDATE SET *
                                WHEN NOT MATCHED THEN
                                    INSERT *"""

        spark.sql(merge_new_data_query)

        get_new_highest_timestamp = f"SELECT MAX(LastEditedWhen) FROM {schema_to_read}.{table_name}" # we nemen max lasteditedwhen in je data warehouse, om later te zien waar je moet stoppen
        highest_timestamp = spark.sql(get_new_highest_timestamp).first()[0] # we maken van highest timestamp en string

        # daarna opnieuw de merge_query die iets langer is, maar daarom niet complexer. we gaan verschillende tabellen dynamisch definiëren.
        #eerst willen we nieuwe timestap wegschrijven zie lijn 'USING' met last_loaded timestamp en een nieuwe highest_timestamp. je schrijft rij per rij. je construct zo een tabel
        # als er match is, dan is voor volgende tabelnaam x, volgende datum de nieuwe highest update timestamp. als geen match dan halen we gewoon de nieuwe waarden binnen, (de not match voegen we toe om volledig te zijn, voor het geval er nog geen tabel is, en je dus een tabel moet aanmaken)
        # 'VALUES': geeft aan welke value in tabel moeten gestoken worden bij de not match
        merge_query = f"""
                MERGE INTO {schema_to_read}.EL_data AS target
                USING (SELECT '{table_name}' AS table_name, current_timestamp() as last_loaded, '{highest_timestamp}' AS highest_update_timestamp) AS source
                ON target.table_name = source.table_name
                WHEN MATCHED THEN 
                UPDATE SET target.highest_update_timestamp = source.highest_update_timestamp
                WHEN NOT MATCHED THEN
                INSERT (table_name, last_loaded, highest_update_timestamp)
                VALUES (source.table_name, source.last_loaded, source.highest_update_timestamp)
            """
        spark.sql(merge_query)
    
    # als er geen lasteditedwhen is, dan doen we een full load van uw table (is zelfde code als bovenaan)
    # print lijnen mogen eruit in productie.
    except Exception as e:
        print(f"{schema_to_read}.{table_name} does not have a LastEditedWhen column. Doing full load...")
        data_df = spark.read.jdbc(url=URL, table=get_full_data_query, properties=ConnectionProperties)
        data_df.write.format("delta").mode("overwrite").save(delta_table_path)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {schema_to_read}.{table_name} USING DELTA LOCATION '{delta_table_path}'")
        print("Load succeeded.")
        pass



# COMMAND ----------

# nog even testen: sales.orders
select * from sales.orders ORDER BY LastEditedWhen DESC

# COMMAND ----------

# nog even testen: Extract + Load data
select * from sales.EL_data	

# COMMAND ----------

# je kan testen door een cel in de sqlserver toe te voegen en daarna bovenstaande code laten draaien. er zou dan voor de tabel een rij moeten verschijnen en je zou verschil moeten zien in el_data bij higest_update_timestamp
