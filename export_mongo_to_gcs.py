import os
import json
import subprocess
from google.cloud import storage

# Set your MongoDB database name
MONGO_DB = "myNewDB"

# Google Cloud Storage Bucket
GCS_BUCKET = "aisa-south-storage"

# Collections to export
COLLECTIONS = ["movies", "series", "tvshows"]

# Local directory to store dumps
EXPORT_DIR = "/tmp/mongo_exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

# Fields that should always be lists
LIST_FIELDS = {"genres", "cast", "creators", "directors"}  # Added 'directors' field

def clean_document(doc):
    """Recursively cleans MongoDB documents for BigQuery compatibility."""
    def fix_keys(obj):
        """Recursively replaces numeric keys with valid field names."""
        if isinstance(obj, dict):
            new_obj = {}
            for key, value in obj.items():
                new_key = f"field_{key}" if key.isdigit() else key
                new_obj[new_key] = fix_keys(value)
            return new_obj
        elif isinstance(obj, list):
            return [fix_keys(item) for item in obj]
        else:
            return obj

    # Convert `_id` from ObjectId to string
    if "_id" in doc and isinstance(doc["_id"], dict) and "$oid" in doc["_id"]:
        doc["_id"] = doc["_id"]["$oid"]

    # If document is nested inside a key like "0", extract it
    if len(doc) == 1 and list(doc.keys())[0].isdigit():
        doc = list(doc.values())[0]

    # Ensure list fields are properly formatted as arrays
    for field in LIST_FIELDS:
        if field in doc and isinstance(doc[field], str):  # Convert "Crime, Drama" → ["Crime", "Drama"]
            doc[field] = [item.strip() for item in doc[field].split(",")]

    return fix_keys(doc)

def export_collection(collection):
    """Exports a MongoDB collection to JSON in NDJSON format with cleaned fields."""
    raw_dump_file = os.path.join(EXPORT_DIR, f"{collection}_raw.json")
    cleaned_dump_file = os.path.join(EXPORT_DIR, f"{collection}.json")

    # Step 1: Export collection from MongoDB
    cmd = [
        "mongoexport",
        "--db", MONGO_DB,
        "--collection", collection,
        "--out", raw_dump_file,
        "--type=json"
    ]
    
    subprocess.run(cmd, check=True)

    # Step 2: Read and clean JSON to fix invalid fields
    with open(raw_dump_file, "r", encoding="utf-8") as infile, \
         open(cleaned_dump_file, "w", encoding="utf-8") as outfile:
        for line in infile:
            doc = json.loads(line)
            cleaned_doc = clean_document(doc)
            outfile.write(json.dumps(cleaned_doc) + "\n")  # NDJSON format

    return cleaned_dump_file

def upload_to_gcs(local_file, gcs_path):
    """Uploads a file to Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_file)
    print(f"Uploaded {local_file} to gs://{GCS_BUCKET}/{gcs_path}")

def main():
    for collection in COLLECTIONS:
        print(f"Exporting {collection}...")
        json_file = export_collection(collection)
        
        # Upload to GCS
        gcs_path = f"mongo_exports/{collection}.json"
        upload_to_gcs(json_file, gcs_path)

        print(f"Exported and uploaded {collection} successfully.")

if __name__ == "__main__":
    main()
