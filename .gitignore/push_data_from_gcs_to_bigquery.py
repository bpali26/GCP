#The following code pushes csv from GCS to BigQuery using:
#1. local folder containing datestamped marked csvs (to extract table schema and column names)
#2. GCP bigquery Rest API in Python 
#3. GCP "bq" command line tool

from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import os
import sys
import glob

#Location paths
local_folder='reports/'
gcs_path="gs://gcs_csv_bucket/"
bq_project="reports_project_name"
dataset_name="reports_dataset"
folder_path=local_folder   #absolute path for folder

#creating date variable
try:
    days=int(sys.argv[1]) # desired date = today - days e.g. days = 1 will push yesterday's data
except:
    days=1  

name_date=(str(datetime.now()-timedelta(days)).replace(" ","_")).split("_")[0].replace("-","_")
client = bigquery.Client()
dataset_ref = client.dataset(dataset_name)
dataset = bigquery.Dataset(dataset_ref)
dataset.description = 'created csv reports for demo'   

#Creating a Fresh dataset in bigquery to load data into new tables
try:
    dataset = client.create_dataset(dataset)  #creating a dataset using python API  
    for file in glob.glob(str(folder_path) + "*" + name_date + ".csv"):  
        gcs_csv = "{}{}".format(gcs_path,file.split("/")[-1])
        table_name=file.split("/")[-1].split("_")[0]
        bqtable = "{}:{}.{}".format(bq_project,dataset_name,table_name)
        read_file=pd.read_csv(file)
        col_names=list(read_file.columns)
#        col_names=[i.replace(" ","_") for i in col_names]  #in case column name contain white spaces
        col_types=[str(x) for x in list(read_file.dtypes)]
        table_ref = dataset.table(table_name)
        table = bigquery.Table(table_ref)
        schema_list=[None]*len(col_names)
        for j in range(len(col_types)):
            schema_list[j]=bigquery.SchemaField(col_names[j],col_types[j],mode='nullable')
        #Normal format: table.schema = [bigquery.SchemaField(col_names, col_types, mode='required'),bigquery.SchemaField('age', 'INTEGER', mode='required')]
        table.schema = schema_list	 #setting table schema 
        table = client.create_table(table)        #creating a table with template
        def load_data_from_gcs(dataset_id, table_id, source):
            bigquery_client = bigquery.Client()
            dataset_ref = bigquery_client.dataset(dataset_id)
            table_ref = dataset_ref.table(table_id)
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = 'CSV'
            job_config.skip_leading_rows = 1
            job = bigquery_client.load_table_from_uri(source, table_ref, job_config=job_config)
            job.result()
            print('Loaded {} rows into {}:{}.'.format(job.output_rows, dataset_id, table_id))        
        dataset_id=dataset_name
        table_id=table_name
        load_data_from_gcs(dataset_id, table_id, gcs_csv) 
#for creating table to already existing dataset
except:
    for file in glob.glob(str(folder_path) + "*" + name_date + ".csv"):
        gcs_csv = "{}{}".format(gcs_path,file.split("/")[-1])
        table_name = file.split("/")[-1].split("_")[0]
        bqtable = "{}:{}.{}".format(bq_project,dataset_name,table_name)
        read_file = pd.read_csv(file)
        col_names = list(read_file.columns)
#        col_names=[i.replace(" ","_") for i in col_names]  #in case column name contain white spaces
        col_types=[str(x) for x in list(read_file.dtypes)]
        try:                     
            table_ref = dataset.table(table_name)  #for creating table first time and then loading data
            table = bigquery.Table(table_ref)
            schema_list=[None]*len(col_names)
            for j in range(len(col_types)):
                schema_list[j] = bigquery.SchemaField(col_names[j],col_types[j],mode='nullable')
            #Normal format: table.schema = [bigquery.SchemaField(col_names, col_types, mode='required'),bigquery.SchemaField('age', 'INTEGER', mode='required')]
            table.schema = schema_list	  #setting table schema 
            table = client.create_table(table)     #creating a table with template
            def load_data_from_gcs(dataset_id, table_id, source):
                bigquery_client = bigquery.Client()
                dataset_ref = bigquery_client.dataset(dataset_id)
                table_ref = dataset_ref.table(table_id)
                job_config = bigquery.LoadJobConfig()
                job_config.source_format = 'CSV'
                job_config.skip_leading_rows = 1
                job = bigquery_client.load_table_from_uri(source, table_ref, job_config=job_config)
                job.result()
                print('Loaded {} rows into {}:{}.'.format(job.output_rows, dataset_id, table_id))        
            dataset_id = dataset_name
            table_id = table_name
            load_data_from_gcs(dataset_id, table_id, gcs_csv) #pushing gcs data into table template
        #add gcs data row wise mentioning all(previous and additional) column names and type to already existing table
        except:        
            #creating col_name_type string
            col_name_type=""
            for j in range(len(col_names)):
                if j!=0:
                    col_name_type+=","+col_names[j]+":"+col_types[j]
                else:
                    col_name_type+=col_names[j]+":"+col_types[j]
            #additional columns and data pushing starts
            load_cmd = "bq load --allow_quoted_newlines --allow_jagged_rows --skip_leading_rows=1 --field_delimiter=',' {} {} {}".format(bqtable, gcs_csv,col_name_type)               
            os.system(load_cmd)   
        print("Completed transporting {} Report for desired date range".format(table_name))
          
os.system("sudo rm -R "+str(folder_path))  #removing local files from VM
print("Completed pushing data into bigquery {} dataset".format(dataset_name))


