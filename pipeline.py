import pandas as pd 
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import os 
from urllib.parse import urlparse
from time import time
url = input("enter your url:",)
df = pd.read_parquet(url,engine='pyarrow')
filename = os.path.basename(urlparse(url).path)

print('total rows to be inseterd in database from {} = {}'.format(filename,len(df)))
df.to_parquet(filename)

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()
query ="""select 1 as num;"""
test = pd.read_sql(query,con=engine)
print(test.head())
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

df.head(n=0).to_sql(name='yellow_taxi', con=engine, if_exists='replace')

#Reading file in batches:
parquet_file = pq.ParquetFile(filename)
i=1

for batch in parquet_file.iter_batches(batch_size=100000):
    print("Batch-{}".format(i))
    batch_df = batch.to_pandas()
    t_start = time()
    batch_df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    batch_df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    batch_df.to_sql(name='yellow_taxi', con=engine, if_exists='append')
    t_end = time()
    print('inserted another chunk, took %.3f second' % (t_end - t_start))
    i+=1
    






