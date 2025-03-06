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
def transform_document(doc, collection_name):
    if not doc:
        return {}

    # Ensure we get the actual nested document and not the _id
    if isinstance(doc, dict) and len(doc) > 1:
        nested_doc = next((v for k, v in doc.items() if isinstance(v, dict)), doc)
    else:
        nested_doc = doc

    # Convert ObjectId to string
    transformed_data = {
        "_id": str(doc["_id"]) if "_id" in doc else "",  
        "title": nested_doc.get("title", ""),  
        "genres": ", ".join(nested_doc.get("genres", [])) if "genres" in nested_doc else "",  
        "year": nested_doc.get("year", None),  
        "type": nested_doc.get("type", ""),  
        "cast": ", ".join(nested_doc.get("cast", [])) if "cast" in nested_doc else "",  
    }

    if collection_name == "movies":
        transformed_data.update({
            "runtime": nested_doc.get("runtime", None),  
            "rated": nested_doc.get("rated", ""),  
            "directors": ", ".join(nested_doc.get("directors", [])) if "directors" in nested_doc else "",  
        })
    elif collection_name in ["series", "tvshows"]:
        transformed_data.update({
            "seasons": nested_doc.get("seasons", None),  
            "creators": ", ".join(nested_doc.get("creators", [])) if "creators" in nested_doc else "",  
        })

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
                if "fullDocument" not in change:
                    print("No fullDocument found in change event:", change)
                    continue

                print("Raw document from MongoDB:", document)
                transformed_data = transform_document(document,collection_name)
                print("Transformed Data:", transformed_data)
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
