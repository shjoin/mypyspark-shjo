import sys
from pyspark.sql import *
#from lib.logger import Log4j
from utils import *


if __name__ == "__main__":   
    confobj =get_spark_app_config()
    spark = SparkSession.builder \
           .config(conf=confobj) \
           .getOrCreate()

    conf_out = spark.sparkContext.getConf()        
    file_name='C:/github/mypyspark-shjo/data'
    file_df= read_csv_df(spark,file_name)
    file_df.show(10)
      
