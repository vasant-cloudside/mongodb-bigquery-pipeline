from pymongo import MongoClient
from google.cloud import bigquery
import threading

# MongoDB connection
MONGO_URI = "mongodb://localhost:27017"
client = MongoClient(MONGO_URI)
db = client["myNewDB"]  # Replace with your actual database name

# BigQuery client setup
BQ_CLIENT = bigquery.Client()
BQ_DATASET = "dataset1409"

# Collection to BigQuery Table Mapping
collections = {
    "movies": "movies_table",
    "series": "series_table",
    "tvshows": "tvshows_table"
}

# Function to transform MongoDB document
def transform_document(document):
    transformed_data = {
        "_id": str(document["_id"]),
        "title": document.get("title", ""),
        "genres": ", ".join(document.get("genres", [])),  # Convert list to string
        "year": document.get("year", None),
        "type": document.get("type", "")
    }

    # Handle additional fields based on collection type
    if "runtime" in document:
        transformed_data["runtime"] = document["runtime"]
    if "rated" in document:
        transformed_data["rated"] = document["rated"]
    if "directors" in document:
        transformed_data["directors"] = ", ".join(document["directors"])
    if "cast" in document:
        transformed_data["cast"] = ", ".join(document["cast"])
    if "seasons" in document:
        transformed_data["seasons"] = document["seasons"]
    if "creators" in document:
        transformed_data["creators"] = ", ".join(document["creators"])

    return transformed_data

# Function to write transformed data to BigQuery
def write_to_bigquery(table_name, data):
    table_id = f"{BQ_CLIENT.project}.{BQ_DATASET}.{table_name}"
    
    errors = BQ_CLIENT.insert_rows_json(table_id, [data])
    if errors:
        print(f"Failed to insert into {table_name}: {errors}")
    else:
        print(f"Inserted into {table_name}: {data}")

# Function to watch MongoDB Change Streams for a collection
def watch_collection(collection_name, table_name):
    collection = db[collection_name]
    with collection.watch() as stream:
        for change in stream:
            if change["operationType"] in ["insert", "update", "replace"]:
                document = change["fullDocument"]
                transformed_data = transform_document(document)
                write_to_bigquery(table_name, transformed_data)

# Start watching collections in separate threads
threads = []
for collection_name, table_name in collections.items():
    thread = threading.Thread(target=watch_collection, args=(collection_name, table_name))
    thread.start()
    threads.append(thread)

# Keep threads running indefinitely
for thread in threads:
    thread.join()
