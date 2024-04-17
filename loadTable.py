#$env:GOOGLE_APPLICATION_CREDENTIALS="C:\github\mypyspark-shjo\gcp_poc\BQ\BQ_PROJECT_WORK\withPy_BQ\mydatabricksproject-ee084f92fdc7.json"

def load_table_Append_csv(table_id): 
    from google.cloud import bigquery
     # Construct a BigQuery client object.
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("Date_time", "DATETIME"),
            bigquery.SchemaField("Age", "INT64"),
            bigquery.SchemaField("Gender", "STRING"),
            bigquery.SchemaField("Country", "STRING"),
            bigquery.SchemaField("state", "STRING"),         
            bigquery.SchemaField("family_history", "STRING"),
            bigquery.SchemaField("treatment", "STRING"),
            bigquery.SchemaField("work_interfere", "STRING"),
            bigquery.SchemaField("MinIncome", "FLOAT64"),
         
        ],
       ) 
        
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,        
    )
        
    uri = "gs://source-data-bucket-shjo/sample_data.csv"
        
        
    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.
    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))


def load_table_Append_table(table_id): 
    from google.cloud import bigquery
     # Construct a BigQuery client object.
    client = bigquery.Client()
    query = """
        SELECT Gender,
        SUM(MinIncome) as SUM_MinIncome
        FROM `mydatabricksproject.bq_dataset.sample` GROUP BY Gender
    """
    query_job= client.query(query)
    print("the query data:" , type(query_job))
    for row in query_job:
        print("Gender={}, SUM_MinIncome={}".format(row[0],row["SUM_MinIncome"]))

def load_query_destination_table(table_id): 
    from google.cloud import bigquery
    dest_table_id ='mydatabricksproject.bq_dataset.sample_agg'
    table_id2 ='mydatabricksproject.bq_dataset.sample'
     # Construct a BigQuery client object.     
    client = bigquery.Client()   
 

    '''
    query = """
        SELECT Gender,
        SUM(MinIncome) as T_MinIncome
        FROM `mydatabricksproject.bq_dataset.sample` GROUP BY Gender
    """
    #query2 ="SELECT Gender,SUM(MinIncome) as T_MinIncome FROM mydatabricksproject.bq_dataset.sample_agg GROUP BY Gender"
    '''

    query2 = """
        SELECT Gender,
        SUM(MinIncome) as T_MinIncome
        FROM `""" +  table_id2 + "`" """
        GROUP BY Gender
    """        
    #WRITE_APPEND ,WRITE_TRUNCATE
    
    
    job_config = bigquery.QueryJobConfig(destination=dest_table_id)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    
    # Start the query, passing in the extra configuration.
    query_job = client.query(query2, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.
      
    print("Query results loaded to the table {}".format(dest_table_id))


if "__name__ == __main__":
  print('Hello,We are in Home(loadTable.py)')
  table_id='mydatabricksproject.bq_dataset.sample'
  dest_table_id ='mydatabricksproject.bq_dataset.sample_agg'
  #load_table_Append_csv(table_id)
  #load_table_Append_table(table_id)
  load_query_destination_table(table_id)