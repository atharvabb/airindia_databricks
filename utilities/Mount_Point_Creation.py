# Databricks notebook source
try:
    dbutils.fs.mount(
    source='wasbs://bronze@airindia7x4.blob.core.windows.net',
    mount_point='/mnt/7x4/bronze',
    extra_configs={'fs.azure.account.key.airindia7x4.blob.core.windows.net':dbutils.secrets.get('7x4','7x4-adlsg2-acccess-key')}
    )
except Exception as e:
    if "Directory already mounted" in str(e):
        print("True")
    else:
        raise e

# COMMAND ----------

try:
    dbutils.fs.mount(
    source='wasbs://gold@airindia7x4.blob.core.windows.net',
    mount_point='/mnt/7x4/gold',
    extra_configs={'fs.azure.account.key.airindia7x4.blob.core.windows.net':dbutils.secrets.get('7x4','7x4-adlsg2-acccess-key')}
    )
except Exception as e:
    if "Directory already mounted" in str(e):
        print("True")
    else:
        raise e

# COMMAND ----------

try:
    dbutils.fs.mount(
    source='wasbs://silver@airindia7x4.blob.core.windows.net',
    mount_point='/mnt/7x4/silver',
    extra_configs={'fs.azure.account.key.airindia7x4.blob.core.windows.net':dbutils.secrets.get('7x4','7x4-adlsg2-acccess-key')}
    )
except Exception as e:
    if "Directory already mounted" in str(e):
        print("True")
    else:
        raise e
