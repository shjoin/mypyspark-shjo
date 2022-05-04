import sys
from pyspark.sql import *
#from lib.logger import Log4j
from utils import *


if __name__ == "__main__":   
    confobj =get_spark_app_config()
    spark = SparkSession.builder \
           .config(conf=confobj) \
           .getOrCreate()    

    #conf_out = spark.sparkContext.getConf()        
    file_name='C:/github/mypyspark-shjo/data/sample.csv'
    #file_name='gs://gcp-storage-bucket-shjo/sample.csv'
    print('Reading Starts......')
    file_df = read_csv_df(spark, file_name)
    #file_df.show(10)
      
    file_df.createOrReplaceTempView("survey_tbl")
    countDF = spark.sql("select Country, count(1) as count from survey_tbl where Age<40 group by Country")

    countDF.show()
    print('Reading complete ......')
