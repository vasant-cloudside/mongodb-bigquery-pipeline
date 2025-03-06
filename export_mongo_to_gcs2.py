import os
import json
import subprocess
from google.cloud import storage

# MongoDB Database Name
MONGO_DB = "myNewDB"

# Google Cloud Storage Bucket
GCS_BUCKET = "aisa-south-storage"

# Collections to export
COLLECTIONS = ["movies", "series", "tvshows"]

# Directory to store JSON files
EXPORT_DIR = "/tmp/mongo_exports"
os.makedirs(EXPORT_DIR, exist_ok=True)


def ensure_list(value):
    """Converts a comma-separated string into a list, ensuring all values are arrays."""
    if isinstance(value, str):
        return [item.strip() for item in value.split(",")]  # Convert comma-separated string to list
    elif isinstance(value, list):
        return value  # Already a list
    elif value is None:
        return []  # Return empty list for None
    else:
        return [value]  # Convert single values into a list


def clean_document(doc):
    """Cleans MongoDB documents for BigQuery compatibility."""
    
    def fix_keys(obj):
        """Recursively replaces numeric keys with valid field names and removes extra wrappers."""
        if isinstance(obj, dict):
            # If dict has only one key and it's numeric, unwrap it
            keys = list(obj.keys())
            if len(keys) == 1 and keys[0].isdigit():
                obj = obj[keys[0]]  # Extract inner document
            
            # Process the dictionary
            new_obj = {}
            for key, value in obj.items():
                new_key = f"field_{key}" if key.isdigit() else key  # Fix numeric keys
                new_obj[new_key] = fix_keys(value)
            return new_obj
        elif isinstance(obj, list):
            return [fix_keys(item) for item in obj]
        else:
            return obj

    # Unwrap `field_0` if it's present
    if isinstance(doc, dict) and "field_0" in doc:
        doc = doc["field_0"]

    # Convert `_id` from ObjectId to string
    if "_id" in doc and isinstance(doc["_id"], dict) and "$oid" in doc["_id"]:
        doc["_id"] = doc["_id"]["$oid"]

    # Ensure `genres`, `cast`, `creators`, and `directors` are arrays
    array_fields = {"genres", "cast", "creators", "directors"}
    for field in array_fields:
        if field in doc:
            if isinstance(doc[field], str):
                # Convert comma-separated string to array
                doc[field] = [item.strip() for item in doc[field].split(",")]
            elif isinstance(doc[field], list):
                # Ensure it's a list
                doc[field] = doc[field]
            else:
                # Skip this document if the field is not a string or list
                print(f"Skipping document {doc['_id']}: Field '{field}' is not a string or list.")
                return None

    return fix_keys(doc)


def export_collection(collection):
    """Exports a MongoDB collection to JSON, cleans the data, and writes the cleaned output."""
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

    # Step 2: Read and clean data
    with open(raw_dump_file, "r", encoding="utf-8") as infile, open(cleaned_dump_file, "w", encoding="utf-8") as outfile:
        for line in infile:
            try:
                doc = json.loads(line)
                cleaned_doc = clean_document(doc)
                if cleaned_doc is not None:  # Skip invalid documents
                    json.dump(cleaned_doc, outfile)
                    outfile.write("\n")  # Ensure each document is on a new line
            except json.JSONDecodeError as e:
                print(f"Skipping invalid JSON line in {collection}: {e}")

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
