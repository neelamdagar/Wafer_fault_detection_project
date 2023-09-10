
from pymongo.mongo_client import MongoClient
import pandas as pd
import json 

# unform resource identifier
uri = "mongodb+srv://neelam:mahi@cluster0.nlngiph.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri)

# create the database and collection objects
database_name="SKILL"
collection_name="waferfault"
# read data as a dataframe
df=pd.read_csv(r"C:\Users\neela\OneDrive\Desktop\sensor2-main\notebooks\wafer_23012020_041211.csv")
df=df.drop('Unnamed: 0',axis=1)
# convert the data csv to json
# json_records=list(json.load(df.T.to_json()).values())
json_record=list(json.loads(df.T.to_json()).values())
# know dump the data into database
client[database_name][collection_name].insert_many(json_record)

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
