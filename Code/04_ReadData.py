# Databricks notebook source
dbutils.fs.ls('/mnt/movies_raw/')

# COMMAND ----------

df_OpenGate = spark.read.option("header", "true").csv("/mnt/movies_raw/OpenGate/2020/03/24/Movies.csv")
df_OpenGate.show()
#This sample code provided by BigDataAzure.com / ItsEasyLearning@gmail.com through course on Udemy: 
#    https://www.udemy.com/course/azure-big-data-mini-project/?referralCode=5435C045B2BFE9747CF0

# COMMAND ----------

df_WestHighway = spark.read.format("csv").option("header", "true").load("/mnt/movies_raw/WestHighway/2020/03/23/Movies.csv")
df_WestHighway.show()


# COMMAND ----------

df_BigBrother = spark.read.json("/mnt/movies_raw/BigBrother/2020/03/25/")
#df_BigBrother.show()
display(df_BigBrother)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col, explode, size
df_actors = df_BigBrother.select(explode(col('actors')).alias('actor'), 'Title')
df_actors = df_actors.select('actor.name','Title')
df_actors.show()

# COMMAND ----------

#Merge data

from pyspark.sql.functions import lit

df_OpenGate = df_OpenGate.select('MovieID','MovieTitle','Category','ReleaseDate').withColumn('Source',lit('OpenGate'))
df_WestHighway = df_WestHighway.select('MovieID','MovieTitle','Category','ReleaseDate').withColumn('Source',lit('WestHighway'))
df_BigBrother = df_BigBrother.select('id','title','genre','availabilityDate').withColumn('Source',lit('BigBrother'))

df_final = df_OpenGate.union(df_WestHighway)
df_final = df_final.union(df_BigBrother)
print(df_OpenGate.count(), df_WestHighway.count(), df_BigBrother.count(), df_final.count())

# COMMAND ----------

df_final.createOrReplaceTempView("TBL_Movies")
display(spark.sql("SELECT Source, Count(*) FROM TBL_Movies Group By Source"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Source, Count(*) FROM TBL_Movies Group By Source

# COMMAND ----------

# MAGIC %md
# MAGIC Read Excel with 3rd party library

# COMMAND ----------

sPath = "/mnt/movies_raw/WestHighway/2020/03/23/Movies_2017_Q1.xlsx"

df_movies_2017 = spark.read.format("com.crealytics.spark.excel").option("location", sPath).option("useHeader", "true").option("treatEmptyValuesAsNulls", "false").option("inferSchema", "false").option("addColorColumns", "false").option("timestampFormat", "dd-MM-yyyy").load(sPath)
print(df_movies_2017.count())
df_movies_2017.show()

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql.functions import udf
def convertToDate(value):
  return datetime.fromordinal(datetime(1900, 1, 1).toordinal() + value - 2).strftime("%Y/%m/%d")
sf_convertToDate = udf(convertToDate)

#This sample code provided by BigDataAzure.com / ItsEasyLearning@gmail.com through course on Udemy: 
#    https://www.udemy.com/course/azure-big-data-mini-project/?referralCode=5435C045B2BFE9747CF0

df = df_movies_2017.select(expr("substring(ReleaseDate, 1, length(ReleaseDate)-2)").cast('int').alias('ReleaseDate'))
df = df.withColumn('ReleaseDate2', sf_convertToDate(col("ReleaseDate"))).select('ReleaseDate2')
df.show()

# COMMAND ----------

#Write to curated layer

df_final.write.mode("overwrite").parquet('abfss://targetmovies@moviescurated.dfs.core.windows.net/movies')

# COMMAND ----------

# MAGIC %md
# MAGIC Read excel using Panda

# COMMAND ----------

import pandas as pd
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

from azure.storage.blob import BlockBlobService, PublicAccess
from azure.storage.blob import AppendBlobService

blobService = BlockBlobService(account_name = "<your account name>", account_key = "<your account key>", sas_token = None)

blobfilepath = "WestHighway/Movies_2017_Q1.xlsx"
arr_2 = blobfilepath.rsplit("/", 1)
localfilename = arr_2[1]
blobService.get_blob_to_path("source-movies",blobfilepath,localfilename)

# MovieID|          MovieTitle|        Category|Rating|RunTimeMin|ReleaseDate

dataSchema = StructType([
  StructField("MovieID",StringType(),False),
  StructField("MovieTitle",StringType(),False),
  StructField("Category",StringType(),True),
  StructField("Rating",StringType(),True),
  StructField("RunTimeMin",StringType(),True),
  StructField("ReleaseDate",StringType(),True)
])

#localfilename = "Movies_2017_Q1.xlsx"
data_xls = pd.read_excel(localfilename, sheet_name="sheet1" , na_filter=False, converters={'ReleaseDate':str})
                             #converters={'Campaign Start': str, 'Campaign End': str, 'Engagement %': str, 'CPC': str,
                             #            'Reach': str, 'CTR (from Ad)': str, 'Impression': str})

data = spark.createDataFrame(data_xls, schema=dataSchema)

print(data.count())
data.show(20,truncate=False)

# COMMAND ----------

from azure.storage.blob import BlockBlobService, PublicAccess
from azure.storage.blob import AppendBlobService

blobService = BlockBlobService(account_name = "<your account>", account_key = "<your account key>", sas_token = None)
#This sample code provided by BigDataAzure.com / ItsEasyLearning@gmail.com through course on Udemy: 
#    https://www.udemy.com/course/azure-big-data-mini-project/?referralCode=5435C045B2BFE9747CF0
blobfilepath = "WestHighway/Movies_2017_Q1.xlsx"
arr_2 = blobfilepath.rsplit("/", 1)
localfilename = arr_2[1]
blobService.get_blob_to_path("source-movies",blobfilepath,localfilename)
print(localfilename)
print(blobfilepath)
