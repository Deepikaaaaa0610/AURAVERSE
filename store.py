# store.py
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()  # loads your .env file

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "dynamic_etl"
SCHEMA_COLLECTION = "schemas"
DATA_COLLECTION = "docs"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

def save_doc(collection: str, doc: dict):
    db[collection].insert_one(doc)

def get_schema():
    s = db[SCHEMA_COLLECTION].find_one({"_id": "global_schema"})
    return s.get("schema") if s else {}

def save_schema(schema: dict):
    db[SCHEMA_COLLECTION].update_one(
        {"_id": "global_schema"},
        {"$set": {"schema": schema}},
        upsert=True
    )

def get_all_docs():
    """Retrieves all documents from the data collection, excluding MongoDB's _id for clean JSON."""
    # Find all documents in the 'docs' collection and convert the cursor to a list
    return list(db[DATA_COLLECTION].find({}, {"_id": 0}))
