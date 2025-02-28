import os, sys, logging
import snowflake.connector
import requests

from dotenv import load_dotenv


load_dotenv()
from cryptography.hazmat.primitives import serialization

logging.basicConfig(level=logging.WARN)

def connect_snow():
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n)"
    p_key = serialization.load_pem_private_key(
        bytes(private_key, 'utf-8'),
        password=None
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        schema="INGEST",
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'py-snowpipe'}, 
    )


Github_CSV = "https://raw.githubusercontent/D-K111/S_Ingestion/blob/main/Product_dim.csv"

Csv_path = '/tmp/dk.csv'
Response = request.get(Github_CSV)

if response.status_code == 200:
    with open (Csv_path,"WB") as d
         d.write (response.content)
    Print("Going Good")

else 
    print("going bad")

exit()    

conn = connect_snow()
connect.cursor().execute("PUT File://{Csv_path} @%Product") 

pp_name= 'Product_pipe'
connect.cursor().execute("alter pipe {pp_name} refresh")
conn.close()
