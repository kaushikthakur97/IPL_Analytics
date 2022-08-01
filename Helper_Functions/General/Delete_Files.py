# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### For Deleting Files or Folders --> Need to bind it in a function and make it dynamic with the extensions

# COMMAND ----------

path = "dbfs:/FileStore/tables/IPL_RAW_DATA/"
original_count = 0
deleted_count = 0
# tuple unpacking  by checking for files/folders in directory
for path,file,size,modTime in dbutils.fs.ls(path):
  original_count += 1
  if file.endswith('.json'):
    deleted_count += 1
    print(f"Deleting {file}...")
    path_to_delete = str(path)
    # deleting file
    dbutils.fs.rm(path_to_delete, True)
  else:
    print("No files/folders to delete")
print(" " * 2)
print(f"Original count in this path {path} was {original_count}")
print(f"Updated count in this path {path} is {original_count - deleted_count}")

# COMMAND ----------

