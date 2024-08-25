import pandas as pd 
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import os 
from urllib.parse import urlparse
import snowflake.connector
from time import time
url = input("enter your url:",)
df = pd.read_parquet(url,engine='pyarrow')
filename = os.path.basename(urlparse(url).path)

##https://ny28059.ap-southeast-2.snowflakecomputing.com
account_identifier = "ny28059.ap-southeast-2"
user = "MKSHARM1011"
password="123456@Home"
DB='YELLOW_TAXI'

con = snowflake.connector.connect(
    user= user,
    password=password,
    account=account_identifier
       
)

result = con.cursor().execute('select current_version()').fetchone()
print(result[0])

print(pd.io.sql.get_schema(df,'test',con=con))


