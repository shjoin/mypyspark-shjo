import pandas as pd
import datetime

file_path ='C:/github/mypyspark-shjo/data'
file_name ='sample.csv'
file_name_out='sample_out'+ '_' + datetime.datetime.now().strftime("%Y%m%d") + '_' + datetime.datetime.now().strftime("%H%M%S") + '.csv'
#file_name='gs://gcp-storage-bucket-shjo/sample.csv'

pd_df=pd.read_csv(file_path + '/' + file_name)
print("FIle read")
#pd_df.head()

#new_pd_df=pd_df.where(pd_df["Age"] < 40).groupby (['Country'])['Country'].count()
#new_pd_df=pd_df.where(pd_df["Age"] < 40).groupby (['Country'])['Age'].aggregate(['min','max'])
new_pd_df=pd_df.where(pd_df["Age"] < 40).groupby (['Country']).aggregate({'name':'count','Age':['min','max']})

print("Aggregated")
new_pd_df.to_csv(file_path + '/' + file_name_out)
print("FIle Saved")
pd_df_read=pd.read_csv(file_path + '/' + file_name_out)
pd_df_read.head()