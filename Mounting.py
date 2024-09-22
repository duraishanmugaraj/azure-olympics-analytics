# Databricks notebook source
!pip install python-dotenv
dbutils.library.restartPython()

# COMMAND ----------

from dotenv import load_dotenv
import os
load_dotenv()
client_secret = os.getenv('client_secret')

# COMMAND ----------


client_id = "a72188ee-cafd-4974-b53f-92d758724c7a"
tenant_id = "f64cb0db-3764-49ad-88e5-053fccfd11f2"

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}

# COMMAND ----------


mount_point = "/mnt/olympic-analytics-data/"
dbutils.fs.mount(
    source="abfss://tokyo-olympics-data@olympicsanalyticsdl.dfs.core.windows.net/",
    mount_point=mount_point,
    extra_configs=configs,
)
print(f"Successfully mounted {mount_point}")

# COMMAND ----------

dbutils.fs.ls(mount_point+'raw')
