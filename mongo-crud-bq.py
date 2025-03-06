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

# Function to update existing records in BigQuery
def update_bigquery(table_name, data):
    table_id = f"{BQ_CLIENT.project}.{BQ_DATASET}.{table_name}"

    query = f"""
    UPDATE `{table_id}`
    SET title = @title,
        genres = @genres,
        year = @year,
        type = @type,
        cast = @cast,
        { "runtime = @runtime," if "runtime" in data else "" }
        { "rated = @rated," if "rated" in data else "" }
        { "directors = @directors," if "directors" in data else "" }
        { "seasons = @seasons," if "seasons" in data else "" }
        { "creators = @creators" if "creators" in data else "" }
    WHERE _id = @id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", data["_id"]),
            bigquery.ScalarQueryParameter("title", "STRING", data["title"]),
            bigquery.ScalarQueryParameter("genres", "STRING", data["genres"]),
            bigquery.ScalarQueryParameter("year", "INT64", data["year"]),
            bigquery.ScalarQueryParameter("type", "STRING", data["type"]),
            bigquery.ScalarQueryParameter("cast", "STRING", data["cast"]),
            bigquery.ScalarQueryParameter("runtime", "INT64", data.get("runtime")) if "runtime" in data else None,
            bigquery.ScalarQueryParameter("rated", "STRING", data.get("rated")) if "rated" in data else None,
            bigquery.ScalarQueryParameter("directors", "STRING", data.get("directors")) if "directors" in data else None,
            bigquery.ScalarQueryParameter("seasons", "INT64", data.get("seasons")) if "seasons" in data else None,
            bigquery.ScalarQueryParameter("creators", "STRING", data.get("creators")) if "creators" in data else None,
        ]
    )

    query_job = BQ_CLIENT.query(query, job_config=job_config)
    query_job.result()  # Wait for the query to finish

    print(f"Updated in {table_name}: {data}")

# Function to delete a record from BigQuery
def delete_from_bigquery(table_name, document_id):
    table_id = f"{BQ_CLIENT.project}.{BQ_DATASET}.{table_name}"

    query = f"DELETE FROM `{table_id}` WHERE _id = @id"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", document_id)
        ]
    )

    query_job = BQ_CLIENT.query(query, job_config=job_config)
    query_job.result()  # Wait for the query to finish

    print(f"Deleted from {table_name}: _id={document_id}")

# Function to watch MongoDB Change Streams for a collection
def watch_collection(collection_name, table_name):
    collection = db[collection_name]
    with collection.watch() as stream:
        for change in stream:
            operation_type = change["operationType"]

            if operation_type in ["insert", "update", "replace"]:
                if "fullDocument" not in change:
                    print("No fullDocument found in change event:", change)
                    continue

                document = change["fullDocument"]
                print("Raw document from MongoDB:", document)

                transformed_data = transform_document(document, collection_name)
                print("Transformed Data:", transformed_data)

                if operation_type == "insert":
                    write_to_bigquery(table_name, transformed_data)
                elif operation_type in ["update", "replace"]:
                    update_bigquery(table_name, transformed_data)

            elif operation_type == "delete":
                document_id = str(change["documentKey"]["_id"])
                delete_from_bigquery(table_name, document_id)

# Start watching collections in separate threads
threads = []
for collection_name, table_name in collections.items():
    thread = threading.Thread(target=watch_collection, args=(collection_name, table_name))
    thread.start()
    threads.append(thread)

# Keep threads running indefinitely
for thread in threads:
    thread.join()
