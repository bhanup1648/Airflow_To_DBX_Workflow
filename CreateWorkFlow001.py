# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

import ast
import astunparse
import json
from pprint import pprint
import base64
from datetime import datetime
import requests
import pandas as pd
import os
import re

# COMMAND ----------

import_statement  = """
from datetime import datetime
import requests
import pandas as pd
import pyspark.pandas as ps
import os
import ast
import astunparse
"""
type_conversion_func = """
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, StringType
from pyspark.sql.types import ArrayType

my_catalogue_name = ""
my_schema_name = ""
my_stg_table_name = ""
dbfs_path = ""

def return_transeformed_df(df):
    sdf = spark.createDataFrame(df)
    #  Convert columns with 'void' data type to 'string'or ArrayType
    for col_name, data_type in sdf.dtypes:
        if data_type == 'void' or isinstance(sdf.schema[col_name].dataType, ArrayType):
            sdf = sdf.withColumn(col_name, sdf[col_name].cast(StringType()))
    return sdf


""" 

# COMMAND ----------

cred_file_path = "/FileStore/bhanu/dag_2_workflow_credentials.json"
dag_path = "/FileStore/bhanu/televison_airflow.py"
json_content = dbutils.fs.head(cred_file_path)
creds_dict = json.loads(json_content)
host = creds_dict['credentials']['host']
token = creds_dict['credentials']['token']
workspace_user = creds_dict['paths']['workspace_user']
user_name = creds_dict['user']['user_name']
existing_cluster_id = creds_dict['cluster']['existing_cluster_id']

# COMMAND ----------

def fetch_function_name(function_string):
    match = re.search(r'def (\w+)', function_string)
    function_name = match.group(1) if match else "Function name not found"
    return function_name

# COMMAND ----------

def replace_objects(content):
    df_to_dbfs = """
        df = df.astype(str)
        sdf = spark.createDataFrame(df)
        sdf.write.parquet(dbfs_path, mode="overwrite")
        spark.sql(f'TRUNCATE TABLE {my_catalogue_name}.{my_schema_name}.{my_stg_table_name}')
        sdf.write.format("delta").mode("overwrite").saveAsTable(f"{my_catalogue_name}.{my_schema_name}.{my_stg_table_name}")
        """
    list_files = """
    files = dbutils.fs.ls(dbfs_folder_path)
    # Extract file names from the list of files
    file_names = [file.name for file in files]
    """
    # ' Replace the specified print statement
    updated_content = content.replace("print('List The Objects From File System')", list_files)
    updated_content = updated_content.replace("print('Move The Files Into Another Path')", "dbutils.fs.mv('dbfs:/tmp/test/test.csv', 'dbfs:/tmp/test2/test2.csv')")
    updated_content = updated_content.replace("print('Read The Csv Files From The File System And Convert to DF')", "spark.read.csv(dbfs_csv_path, header=True, inferSchema=True)")
    # updated_content = updated_content.replace("print('Store DF To File System')", "df.write.csv(dbfs_output_path, mode='overwrite', header=True)")
    updated_content = updated_content.replace("print('Delete The Objects From File System')", "dbutils.fs.rm(dbfs_path_to_delete, recurse=True)")
    updated_content = updated_content.replace("print('Writing a string to FileSystem')", "dbutils.fs.put('dbfs:/FileStore/myfile.txt', my_string, True)")
    updated_content = updated_content.replace("print('Writing df to file system and catalogue table')", df_to_dbfs) 

    return updated_content

# COMMAND ----------

def import_python_file_into_workspace(host,token,workspace_user,context,function_name):        
    # Filter out only Python files
    encoded_content = base64.b64encode(context.encode()).decode()
    notebook_path   =  workspace_user + function_name  
    # REST API request
    response = requests.post(
        f'{host}/api/2.0/workspace/import',
        headers={'Authorization': f'Bearer {token}'},
        json={
            'content': encoded_content,
            'path': notebook_path,
            'language': 'PYTHON',
            'overwrite': True,
            'format': 'SOURCE'
        }
    )

    # Check response
    if response.status_code == 200:
        print(f"{notebook_path + function_name} Notebook created successfully")
    else:
        print(f"{function_name} Failed to create notebook")

# COMMAND ----------

def regex_datetime_2_string_values(default_args):
    pattern = r'(datetime\([^)]+\))'
    # Replace datetime(...) with "datetime(...)"
    new_default_args = re.sub(pattern, r'"\1"', default_args)
    return new_default_args

def fetch_global_string_variables(file_path):
    global_string_vars = {}
    dag_content = dbutils.fs.head(dag_path)
    tree = ast.parse(dag_content, filename=file_path)        
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) :
            for target in node.targets:
                if isinstance(target, ast.Name) and isinstance(node.value, ast.Str):
                    global_string_vars[target.id] = node.value.s
                elif isinstance(target, ast.Name) and isinstance(node.value, ast.Dict):
                    # unparse 
                    dict_var = astunparse.unparse(node.value)
                    dict_var = dict_var.replace("'",'"') 
                    dict_var =  regex_datetime_2_string_values(dict_var)           
                    global_string_vars[target.id] = dict_var   
    variable_assignments = "\n".join([f"{key} = '{value}'" for key, value in global_string_vars.items()])
    variable_assignments = variable_assignments.replace("""
'""","'")
    # Access the global string variables from the fetched dictionary
    return variable_assignments

# COMMAND ----------

def extract_functions(node, functions):
    if isinstance(node, ast.FunctionDef):
        functions.append(astunparse.unparse(node))

    for child_node in ast.iter_child_nodes(node):
        extract_functions(child_node, functions)

def parse_dag(dag_path):
    notebook_content = dbutils.fs.head(dag_path)
    tree = ast.parse(notebook_content)
    functions = []
    for item in tree.body:
        extract_functions(item, functions)
    return functions

def write_functions_to_file(functions):
    for func_code in functions:
        func_name  = fetch_function_name(func_code)
        # print(func_name)
        # Rename s3 utils to dbutils
        func_code = replace_objects(func_code) 
        # Add  global vars
        global_vars = fetch_global_string_variables(dag_path)
        func_code = global_vars  +  func_code
        #Import statements
        if "api" in func_name:
            func_code = import_statement + type_conversion_func  +  func_code
        else:
            func_code = import_statement + func_code
        # call function
        func_code = f"{func_code}\n{func_name}()"
        # Import Python Functions Into Workspace as a File
        import_python_file_into_workspace(host,token,workspace_user,func_code,func_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Code

# COMMAND ----------

parsed_functions = parse_dag(dag_path)
write_functions_to_file(parsed_functions)

# COMMAND ----------


def extract_task_dependencies(node):
    dependencies = {}

    def extract_from_list(node):
        return [task.id for task in node.elts]

    def extract_dependency(bin_op):
        if isinstance(bin_op.op, ast.RShift):
            if isinstance(bin_op.left, ast.BinOp):
#                 print("inside binary op")
                upstream_tasks = extract_dependency(bin_op.left)
                if isinstance(bin_op.right, ast.List):
                    downstream_tasks = extract_from_list(bin_op.right)
                elif isinstance(bin_op.right, ast.Name):
                    downstream_tasks = [bin_op.right.id]
                else:
                    raise ValueError("Invalid syntax for downstream tasks")

                for downstream_task in downstream_tasks:
                    for upstream_task in upstream_tasks:
                        if upstream_task not in dependencies:
                            dependencies[upstream_task] = {"depends_on": [downstream_task]}
                        else:
                            dependencies[upstream_task]["depends_on"].append(downstream_task)

                return downstream_tasks

            elif isinstance(bin_op.left, ast.List):
#                 print("inside list")
                upstream_tasks = extract_from_list(bin_op.left)
                if isinstance(bin_op.right, ast.List):
                    downstream_tasks = extract_from_list(bin_op.right)
                elif isinstance(bin_op.right, ast.Name):
                    downstream_tasks = [bin_op.right.id]
                else:
                    raise ValueError("Invalid syntax for downstream tasks")

                for downstream_task in downstream_tasks:
                    for upstream_task in upstream_tasks:
                        if upstream_task not in dependencies:
                            dependencies[upstream_task] = {"depends_on": [downstream_task]}
                        else:
                            dependencies[upstream_task]["depends_on"].append(downstream_task)

                return downstream_tasks

            elif isinstance(bin_op.left, ast.Name):
                upstream_tasks = [bin_op.left.id]
                print("upstream_task", upstream_tasks)
                if isinstance(bin_op.right, ast.Name):
                    downstream_tasks = [bin_op.right.id]
                elif isinstance(bin_op.right, ast.List):
                    downstream_tasks = extract_from_list(bin_op.right)
                else:
                    raise ValueError("Invalid syntax for downstream tasks")

                for downstream_task in downstream_tasks:
                    for upstream_task in upstream_tasks:
                        if upstream_task not in dependencies:
                            dependencies[upstream_task] = {"depends_on": [downstream_task]}
                        else:
                            dependencies[upstream_task]["depends_on"].append(downstream_task)

                return downstream_tasks

    def visit_node(current_node):
        if isinstance(current_node, ast.Expr) and isinstance(current_node.value, ast.BinOp):
#             print("node found")
            bin_op = current_node.value
            extract_dependency(bin_op)

    for item in node.body:
        visit_node(item)

    return dependencies

def reverse_dependencies(dependencies):
    reversed_dependencies = {}
    tasks=[]

    for task, info in dependencies.items():
        tasks.append(task)
        upstream_tasks = info.get("depends_on", [])
        
        # Check if depends_on is an empty list
        if not upstream_tasks:
            # If it's empty, keep the key as it is in reversed_dependencies
            reversed_dependencies[task] = {"depends_on": []}
        else:
            # Reverse the dependencies for non-empty depends_on list
            for upstream_task in upstream_tasks:
                if upstream_task not in reversed_dependencies:
                    reversed_dependencies[upstream_task] = {"depends_on": [task]}
                else:
                    reversed_dependencies[upstream_task]["depends_on"].append(task)
        for task in tasks:
            if task not in reversed_dependencies.keys():
                reversed_dependencies[task] = {"depends_on": []}

    return reversed_dependencies


def extract_task_info(node):
    if (
        isinstance(node, ast.Assign)
        and len(node.targets) == 1
        and isinstance(node.targets[0], ast.Name)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id.endswith('Operator')  # Filter only Operator tasks
    ):
        task_id = node.targets[0].id
        operator_info = {
            'operator': node.value.func.id
        }

        for keyword in node.value.keywords:
            arg_name = keyword.arg
            arg_value = extract_arg_value(keyword.value)
            operator_info[arg_name] = arg_value

        return task_id, operator_info

    return None, None

def extract_arg_value(arg_node):
    if isinstance(arg_node, ast.Dict):
        return {
            extract_arg_value(key): extract_arg_value(value) for key, value in zip(arg_node.keys, arg_node.values)
        }
    elif isinstance(arg_node, ast.List):
        return [extract_arg_value(item) for item in arg_node.elts]
    elif isinstance(arg_node, ast.Tuple):
        return tuple(extract_arg_value(item) for item in arg_node.elts)
    elif isinstance(arg_node, ast.Str):
        return arg_node.s
    elif isinstance(arg_node, ast.Num):
        return arg_node.n
    elif isinstance(arg_node, ast.Name):
        return arg_node.id
    elif isinstance(arg_node, ast.Call):
        return extract_task_info(arg_node)[1] if arg_node.func.id.endswith('Operator') else None
    elif isinstance(arg_node, ast.Subscript) and isinstance(arg_node.value, ast.Name) and arg_node.value.id == 'DATA_SETS':
        # Handle dynamic tasks created using a dict
        return f"{{DATA_SETS['{arg_node.slice.value.s}']}}"
    else:
        return None

def parse_dag(dag_path):
    notebook_content = dbutils.fs.head(dag_path)
    return ast.parse(notebook_content)

tree = parse_dag(dag_path)
# print(ast.dump(tree))

dag_info = {}
for node in ast.walk(tree):
    task_id, operator_info = extract_task_info(node)
    if task_id is not None:
        dag_info[task_id] = operator_info

dependencies = extract_task_dependencies(tree)
dependencies = reverse_dependencies(dependencies)
# print(dependencies)
combined_dict = {key: {**dag_info[key], **dependencies[key]} for key in dag_info if key in dependencies}

combined_json = json.dumps(combined_dict, indent=2)
print(combined_json)

# COMMAND ----------

existing_cluster_id = "0104-115443-g3puolpy"
job_name  = "Airflow_DAG_DBX_Workflow"

def import_sql_file_into_workspace(host,token,workspace_user,context):        
    # Filter out only Python files
    encoded_content = base64.b64encode(context.encode()).decode()
    notebook_path   =  workspace_user + "my_postgres_sql"  
    # REST API request
    response = requests.post(
        f'{host}/api/2.0/workspace/import',
        headers={'Authorization': f'Bearer {token}'},
        json={
            'content': encoded_content,
            'path': notebook_path,
            'language': 'SQL',
            'overwrite': True,
            'format': 'SOURCE'
        }
    )

    # Check response
    if response.status_code == 200:
        print(f"{notebook_path} Notebook created successfully")
    else:
        print(f" Failed to create notebook")

def create_databricks_job_json(task_info):
    # Map for converting trigger rule to Databricks-compatible run_if
    run_if_mapping = {
        "none_failed": "ALL_SUCCESS",
    }
    dummy_path = "print_hello"

    tasks = []
    for task_id, info in task_info.items():
        depends_on = []
        for task in info.get("depends_on", ""):
            depends_on.append({"task_key": task})
        task = {
            "task_key": task_id,

            "run_if": run_if_mapping.get(info.get("trigger_rule", ""), "ALL_SUCCESS"),
            "existing_cluster_id": "0104-115443-g3puolpy",
            "timeout_seconds": 0,
            "email_notifications": {},
            "depends_on": depends_on
        }

        if info["operator"] == "PythonOperator":
            task["notebook_task"] = {
                "notebook_path": workspace_user + info.get("python_callable", ""),
                "source": "WORKSPACE"
            }
        elif info["operator"] == "DummyOperator":
            task["notebook_task"] = {
                "notebook_path": workspace_user + dummy_path +".py",  
                "source": "WORKSPACE"
            }
        elif info["operator"] == "CustomOperator":
            # Handle other operators if needed
            pass
        elif info["operator"] == "PostgresOperator":
            sql = "%sql\n" + info.get("sql")
            import_sql_file_into_workspace(host,token,workspace_user,sql)
            task["notebook_task"] = {
                "notebook_path": workspace_user + "my_postgres_sql",  
                "source": "WORKSPACE"
            }

        tasks.append(task)

    job_json_template = {
        "name": "Airflow_DAG_DBX_Workflow",
        "email_notifications": {
            "no_alert_for_skipped_runs": False
        },
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": tasks,
        "run_as": {
            "user_name": "bhanuprakash.pothugunta@kpipartners.com"
        }
        }

    return job_json_template

result_dict = json.loads(combined_json)
task_info_dict = result_dict

databricks_job_json = create_databricks_job_json(task_info_dict)
print(json.dumps(databricks_job_json, indent=4))
request_body =  json.dumps(databricks_job_json, indent=2)

# COMMAND ----------

create_job_url = f"{host}/api/2.0/jobs/create" ##'https://adb-747007169892122.2.azuredatabricks.net/api/2.0/jobs/create'
# update_job_url = f"{host}/api/2.0/jobs/update" ##'https://adb-747007169892122.2.azuredatabricks.net/api/2.0/jobs/create'

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}


# json_settings = json.dumps(job_json)
# encoded_content = base64.b64encode(json_settings.encode()).decode()
# response = requests.post(update_job_url, headers=headers, data=job_json)
# response = requests.post(create_job_url, headers=headers, json=json.dumps(job_json))
response = requests.post(create_job_url, headers=headers, data=request_body)

if response.status_code == 200:
    print("Databricks job created successfully.")
else:
    print(f"Error creating Databricks job. Status code: {response.status_code}")
    print(response.text)

# COMMAND ----------

##reset

# COMMAND ----------

