import astrapy
import pandas as pd
import ast
import uuid

# Initialize the DataStax Astra client

api_url_id = '06f1a9fe-dd6f-442a-ad75-0bada82c97ea'

app_token = (
    'NGEpZmLDxaxXJZqdcZJwBCTT:'
    '6e431cc0726e7a95b67fa1112c2e8a276bdf1975709d0e2b1e9f5df8b199b849'
)
client = astrapy.DataAPIClient(f"AstraCS:{app_token}")
database = client.get_database_by_api_endpoint(
    f"https://{api_url_id}-eu-west-1.apps.astra.datastax.com"
)
collection = database.get_collection("testwikidata")

# Function to convert vector string to list of floats


def convert_vector(vector_str):
    if isinstance(vector_str, str):
        return [float(x) for x in ast.literal_eval(vector_str)]
    elif isinstance(vector_str, float):
        return [vector_str]


# Function to generate documents from CSV rows


def generate_document(row):
    return {
        "_id": str(uuid.uuid4()),  # Unique identifier for each document
        "qid": row["qid"],
        "pid": row["pid"],
        "value": row["value"],
        "item_label": row["item_label"],
        "property_label": row["property_label"],
        "value_content": row["value_content"],
        "statement": row["statement"],
        "vector": convert_vector(row["embedding"])  # Convert string to vector
    }

# Batch insert documents into the collection


def batch_insert_documents(collection, documents):
    collection.insert_many(
        documents,
        vectors=[doc["embedding"] for doc in documents]
    )

# Read CSV in chunks and upload to Astra DB


def upload_csv_to_astra(csv_file, chunk_size=1000):
    for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
        documents = [generate_document(row) for index, row in chunk.iterrows()]
        batch_insert_documents(collection, documents)
        print(f"Inserted {len(documents)} documents")


# Path to the CSV file
csv_file_path = './csvfiles/wikidata_vectordb_datadump_1000_en.csv'

# Upload the CSV data to Astra DB
upload_csv_to_astra(csv_file_path)
