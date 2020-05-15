# Databricks notebook source
# MAGIC %sh
# MAGIC # Put this script inside init file : https://docs.databricks.com/clusters/init-scripts.html
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC apt-get update
# MAGIC ACCEPT_EULA=Y apt-get install msodbcsql17
# MAGIC apt-get -y install unixodbc-dev
# MAGIC sudo apt-get install python3-pip -y
# MAGIC pip3 install --upgrade pyodbc

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC cat /etc/odbcinst.ini

# COMMAND ----------

#This sample code provided by ItsEasyLearning through course on Udemy: 
#    https://www.udemy.com/course/azure-big-data-mini-project/?referralCode=5435C045B2BFE9747CF0
# Using Pandas - Read data from SQL and create Spark dataframe

import pyodbc
import pandas as pd

sqlconn = dbutils.secrets.get("kvfordbx","SQLConnStrForPyodbc")

cnxn = pyodbc.connect(sqlconn)
query = "SELECT * FROM Users"
pdf = pd.read_sql(query, cnxn)
sparkDF =  spark.createDataFrame(pdf)
sparkDF.show()

# COMMAND ----------

# Without using Pandas - Read data from SQL and create Spark dataframe

import pyodbc

sqlconn = dbutils.secrets.get("kvfordbx","SQLConnStrForPyodbc")

cnxn = pyodbc.connect(sqlconn)
query = "SELECT * FROM Users"
cursor = cnxn.cursor()
cursor.execute(query)
rows = cursor.fetchall()
column_names = [x[0] for x in cursor.description]

df = spark.createDataFrame((tuple(r) for r in rows), column_names)

df.show()
'''
row = cursor.fetchone()
while row: 
    print(row)
    row = cursor.fetchone()
'''
cnxn.close()


# COMMAND ----------

#This sample code provided by ItsEasyLearning through course on Udemy: 
#    https://www.udemy.com/course/azure-big-data-mini-project/?referralCode=5435C045B2BFE9747CF0
# Push data to SQL server table

from datetime import datetime
import pyodbc

sqlconn = dbutils.secrets.get("kvfordbx","SQLConnStrForPyodbc")

cnxn = pyodbc.connect(sqlconn)
cursor = cnxn.cursor()

cursor.execute("""
INSERT INTO Users (UserName, FullName) 
VALUES (?,?)""",
'pyodbcuser2', 'pyodbc user2') 
cnxn.commit()

# COMMAND ----------

# DML operation on SQL server table

from datetime import datetime
import pyodbc

sqlconn = dbutils.secrets.get("kvfordbx","SQLConnStrForPyodbc")

cnxn = pyodbc.connect(sqlconn)
cursor = cnxn.cursor()

cursor.execute("DELETE FROM Users WHERE UserId in (6)") 
cnxn.commit()
