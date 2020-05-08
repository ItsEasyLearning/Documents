# Databricks notebook source

#Install azure-storage-blob library (type = PyPI)
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

#Using connection string
blob_service_client = BlobServiceClient.from_connection_string('<blob Storage connectionstring here>')
container_client = blob_service_client.get_container_client('<container_name>')

# Use SAS URL to get container
#sas_url = "https://account.blob.core.windows.net/mycontainer?sv=2015-04-05&st=2015-04-29T22%3A18%3A26Z&se=2015-04-30T02%3A23%3A26Z&sr=b&sp=rw&sip=168.1.5.60-168.1.5.70&spr=https&sig=Z%2FRHIX5Xcg0Mq2rqI3OlWTjEg2tYkboXr1P9ZUXDtkk%3D"
#container = ContainerClient.from_container_url(sas_url)

#This sample code provided by ItsEasyLearning through course on Udemy: 
#    https://www.udemy.com/course/azure-big-data-mini-project/?referralCode=5435C045B2BFE9747CF0


# List the blobs in the container
blob_list = container_client.list_blobs('')
for blob in blob_list:
    print("\t" + blob.name)



# COMMAND ----------

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

#Using connection string
blob_service_client = BlobServiceClient.from_connection_string('<blob Storage connectionstring here>')
container_client = blob_service_client.get_container_client('<container_name>')

#This sample code provided by ItsEasyLearning through course on Udemy: 
#   https://www.udemy.com/course/azure-big-data-mini-project/?referralCode=5435C045B2BFE9747CF0

blob_client = blob_service_client.get_blob_client('bigdata', 'reddit/2020/04/20/20200420092148Z-comments.json')
download_stream = blob_client.download_blob()
fileContents = download_stream.readall()

print(fileContents)

# COMMAND ----------

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

#Using connection string
blob_service_client = BlobServiceClient.from_connection_string('<blob Storage connectionstring here>')
container_client = blob_service_client.get_container_client('<container_name>')

#This sample code provided by ItsEasyLearning through course on Udemy: 
#   https://www.udemy.com/course/azure-big-data-mini-project/?referralCode=5435C045B2BFE9747CF0

blob_client = blob_service_client.get_blob_client('sdk', 'new_append_blob.txt')
print(blob_client)
#blob_client.create_append_blob()

blob_client.append_block("line one\r\n")
blob_client.append_block("line two\r\n")

## Documentation: https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python

