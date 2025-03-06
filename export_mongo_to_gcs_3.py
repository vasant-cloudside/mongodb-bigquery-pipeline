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


def convert_to_array(value):
    """Converts a comma-separated string to a list if necessary."""
    if isinstance(value, str):
        return [item.strip() for item in value.split(",")]
    return value if isinstance(value, list) else []


def clean_document(doc):
    """Flattens MongoDB documents for BigQuery compatibility."""
    
    # Ensure `_id` is converted properly if in ObjectId format
    if "_id" in doc and isinstance(doc["_id"], dict) and "$oid" in doc["_id"]:
        doc["_id"] = doc["_id"]["$oid"]

    # Unwrap single nested objects like `{"0": {...}}`
    for key in list(doc.keys()):  # Iterate over keys
        if isinstance(doc[key], dict) and len(doc[key]) > 1:
            doc.update(doc[key])  # Merge nested dictionary into main document
            del doc[key]  # Remove original nested key

    # Convert specific fields to arrays
    for field in ["genres", "directors", "creators", "cast"]:
        if field in doc:
            doc[field] = convert_to_array(doc[field])

    return doc


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

                # Write cleaned doc in NDJSON format
                json.dump(cleaned_doc, outfile)
                outfile.write("\n")

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
