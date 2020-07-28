# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE dbmovies LOCATION "abfss://curated@<your_account>.dfs.core.windows.net/DBX_Hive";

# COMMAND ----------

# MAGIC %sql 
# MAGIC Show databases;
# MAGIC --drop database temp_dbmovies;

# COMMAND ----------

df_OpenGate = spark.read.option("header", "true").csv("/mnt/movies_raw/OpenGate/2020/04/04/Movies.csv")
df_OpenGate.createOrReplaceTempView("tbl_OpenGate")
#This sample code provided by BigDataAzure.com / ItsEasyLearning@gmail.com through course on Udemy: 
#    https://www.udemy.com/course/azure-big-data-mini-project/?referralCode=5435C045B2BFE9747CF0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tbl_OpenGate

# COMMAND ----------

# MAGIC %sql
# MAGIC use dbmovies;
# MAGIC Create table Movies (MovieId string, MovieTitle string, Category String, Rating string, RunTimeMin string, ReleaseDate string, Source string, CreatedDate timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC use dbmovies;
# MAGIC Insert Into Movies Select MovieID, MovieTitle, Category, Rating, RunTimeMin, ReleaseDate, 'OpenGate', current_timestamp() from tbl_OpenGate;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from Movies

# COMMAND ----------

# MAGIC %sql
# MAGIC use dbmovies;
# MAGIC Create table MoviesDelta (MovieId string, MovieTitle string, Category String, Rating string, RunTimeMin string, ReleaseDate string, Source string, CreatedDate timestamp) using Delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC use dbmovies;
# MAGIC Insert Into MoviesDelta Select MovieID, MovieTitle, Category, Rating, RunTimeMin, ReleaseDate, 'OpenGate', current_timestamp() from tbl_OpenGate;
