# Databricks notebook source
# Definieer een functie om een JDBC connection String te genereren

def get_sql_jdbc_string(server_name: str, database_name: str, user: str, password: str) -> str:
    return f"jdbc:sqlserver://{server_name}:1433;database={database_name};user={user}@{server_name};password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

# Definieer een functie om de jdbc connection properties te generen:

def get_sql_connection_properties(user: str, password: str) -> dict:
    return {"user": user, "password": password, "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
