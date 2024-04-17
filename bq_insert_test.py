from google.cloud import bigquery


print("***************Process Being Started** *************")
try: 
    project_id='mydatabricksproject'
    dataset_id='bq_dataset'
    source_table_id='sample'
    destination_table_id='sample_country_MinIncome_test2'
    
    commit_sql="COMMIT TRANSACTION;"

    sql_query = "INSERT INTO "+project_id + "." +dataset_id+"."+ destination_table_id+" \
                SELECT\
                country,\
                sum(MinIncome)\
                FROM " + project_id + "." +dataset_id+"."+source_table_id +"\
                group by Country;"    
    #print(sql_query)
    client=bigquery.Client()
    query_job=client.query(sql_query)
    query_job2=client.query(commit_sql)
    rows=query_job.result()    
    print("Successfully Completed....")
except Exception as e:
     print(e)   
    #countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age>=35 group by Country")    
    #countDF.write.format('bigquery').option('table', 'mydatabricksproject.bq_dataset.sample_country_cnt').mode("overwrite").save()
    #spark.stop()