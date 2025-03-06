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

    # Extract the nested document (if it exists)
    nested_doc = doc.get("0", {}) if "0" in doc else {}

    # Convert ObjectId to string
    transformed_data = {
        "_id": str(doc["_id"]) if "_id" in doc else "",
        "title": doc.get("title", nested_doc.get("title", "")),  # Prioritize root-level title
        "genres": ", ".join(doc.get("genres", nested_doc.get("genres", []))),  # Prioritize root-level genres
        "year": doc.get("year", nested_doc.get("year", None)),  # Prioritize root-level year
        "type": doc.get("type", nested_doc.get("type", "")),  # Prioritize root-level type
        "cast": ", ".join(doc.get("cast", nested_doc.get("cast", []))),  # Prioritize root-level cast
    }

    if collection_name == "movies":
        transformed_data.update({
            "runtime": doc.get("runtime", nested_doc.get("runtime", None)),  # Prioritize root-level runtime
            "rated": doc.get("rated", nested_doc.get("rated", "")),  # Prioritize root-level rated
            "directors": ", ".join(doc.get("directors", nested_doc.get("directors", []))),  # Prioritize root-level directors
        })
    elif collection_name in ["series", "tvshows"]:
        transformed_data.update({
            "seasons": doc.get("seasons", nested_doc.get("seasons", None)),  # Prioritize root-level seasons
            "creators": ", ".join(doc.get("creators", nested_doc.get("creators", []))),  # Prioritize root-level creators
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

    # Construct the SET clause dynamically based on the data
    set_clause = []
    query_parameters = []

    set_clause.append("title = @title")
    query_parameters.append(bigquery.ScalarQueryParameter("title", "STRING", data["title"]))

    set_clause.append("genres = @genres")
    query_parameters.append(bigquery.ScalarQueryParameter("genres", "STRING", data["genres"]))

    set_clause.append("year = @year")
    query_parameters.append(bigquery.ScalarQueryParameter("year", "INT64", data["year"]))

    set_clause.append("type = @type")
    query_parameters.append(bigquery.ScalarQueryParameter("type", "STRING", data["type"]))

    # Escape the `cast` column name
    set_clause.append("`cast` = @cast")
    query_parameters.append(bigquery.ScalarQueryParameter("cast", "STRING", data["cast"]))

    if "runtime" in data:
        set_clause.append("runtime = @runtime")
        query_parameters.append(bigquery.ScalarQueryParameter("runtime", "INT64", data["runtime"]))

    if "rated" in data:
        set_clause.append("rated = @rated")
        query_parameters.append(bigquery.ScalarQueryParameter("rated", "STRING", data["rated"]))

    if "directors" in data:
        set_clause.append("directors = @directors")
        query_parameters.append(bigquery.ScalarQueryParameter("directors", "STRING", data["directors"]))

    if "seasons" in data:
        set_clause.append("seasons = @seasons")
        query_parameters.append(bigquery.ScalarQueryParameter("seasons", "INT64", data["seasons"]))

    if "creators" in data:
        set_clause.append("creators = @creators")
        query_parameters.append(bigquery.ScalarQueryParameter("creators", "STRING", data["creators"]))

    # Construct the final query
    set_clause_str = ", ".join(set_clause)
    query = f"""
    UPDATE `{table_id}`
    SET {set_clause_str}
    WHERE _id = @id
    """

    # Add the _id parameter
    query_parameters.append(bigquery.ScalarQueryParameter("id", "STRING", data["_id"]))

    # Execute the query
    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    query_job = BQ_CLIENT.query(query, job_config=job_config)
    query_job.result()  # Wait for query execution

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
    with collection.watch(full_document="updateLookup") as stream:
        for change in stream:
            operation_type = change["operationType"]

            if operation_type in ["insert", "update", "replace"]:
                # Fetch the latest document from MongoDB after an update
                document_id = change["documentKey"]["_id"]
                latest_document = collection.find_one({"_id": document_id})

                if not latest_document:
                    print(f"No document found with _id={document_id}")
                    continue

                print("Latest document from MongoDB:", latest_document)

                # Transform the latest document
                transformed_data = transform_document(latest_document, collection_name)
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
