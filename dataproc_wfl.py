from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from google.cloud import bigquery


if __name__ == "__main__":  
    Myconf= SparkConf().setAppName("BQ DataProc") 
    #Myconf.set("spark.master","local[3]") #Or  Myconf.set("spark.app.name","Hello Spark")
    #Myconf.set('spark.jars', 'gs://spark-lib/bigquery/spark-3.1-bigquery-0.27.1-preview.jar') because we are pasing in Dataproc submit
    
    spark = SparkSession.builder.config(conf=Myconf).getOrCreate()  # OR spark = SparkSession.builder.config(conf=conf).getOrCreate()   SparkSession.builder.config(conf=SparkConf()).getOrCreate()
    spark.conf.set("temporaryGcsBucket", "temp_spark-bigquery-connector")
     #Sets a config option. Options set using this method are automatically propagated to both SparkConf and SparkSession’s own configuration.
    #builder.config(key=None, value=None, conf=None)
    #config('spark.jars', 'gs://spark-lib/bigquery/spark-3.1-bigquery-0.27.1-preview.jar')
    #spark = SparkSession.builder.config(conf=conf).getOrCreate()
    #spark.conf.set("temporaryGcsBucket", "temp_spark-bigquery-connector")
    
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
    
    client=bigquery.Client()
    query_job=client.query(sql_query)
    query_job2=client.query(commit_sql)
    rows=query_job.result()
    print("Done")    
    
    #countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age>=35 group by Country")    
    #countDF.write.format('bigquery').option('table', 'mydatabricksproject.bq_dataset.sample_country_cnt').mode("overwrite").save()
    #spark.stop()