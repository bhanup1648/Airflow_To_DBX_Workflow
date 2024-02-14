# Databricks notebook source
arguments = {
    "credentials": {
        "host": "https://adb-747007169892122.2.azuredatabricks.net",
        "token": "dapib95f3ff4d743b96fa60002a926397851-3"
    },
    "paths": {
        "workspace_user": "/Users/bhanuprakash.pothugunta@kpipartners.com/samples/",
        "dag_path": "/FileStore/bhanu/televison_airflow.py"
    },
    "imports": {
        "python_modules": """
        from datetime import datetime
        import requests
        import pandas as pd
        import pyspark.pandas as ps
        import os
        import ast
        import astunparse

        from pyspark.sql.types import StringType
        from pyspark.sql.functions import col, StringType
        from pyspark.sql.types import ArrayType
        """

    },
    "code_attachments": {
        "data_type_conv": """
        def return_transeformed_df(df):
            sdf = spark.createDataFrame(df)
            #  Convert columns with 'void' data type to 'string'or ArrayType
            for col_name, data_type in sdf.dtypes:
                if data_type == 'void' or isinstance(sdf.schema[col_name].dataType, ArrayType):
                    sdf = sdf.withColumn(col_name, sdf[col_name].cast(StringType()))
            return sdf
        """

    }
}

# COMMAND ----------

import json

# Convert dictionary to JSON string with indentation
json_data = json.dumps(arguments, indent=4)

# COMMAND ----------

json_file_path = "/dbfs:/FileStore/bhanu/arguments.json"
with open(json_file_path, 'w') as json_file:
    json_file.write(json_data)

# COMMAND ----------

# import json
cred_file_path = "/FileStore/bhanu/dag_2_workflow_credentials.json"

# Read JSON file
json_content = dbutils.fs.head(cred_file_path)
json_data = json.loads(json_content)
print(json_data)

# COMMAND ----------

json_data['credentials']['host']

# COMMAND ----------

