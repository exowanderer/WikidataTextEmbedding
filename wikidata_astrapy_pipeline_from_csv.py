import astrapy
import numpy as np
import pandas as pd
import ast
import uuid

from tqdm import tqdm
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
    # print(f'{vector_str=}')

    vector_str = vector_str.replace(' ', ',')
    while ',,' in vector_str:
        vector_str = vector_str.replace(',,', ',')

    vector_str = vector_str.replace('[,', '[').replace(',]', ']')

    if isinstance(vector_str, str):
        return [float(x) for x in ast.literal_eval(vector_str)]
    elif isinstance(vector_str, float):
        return [vector_str]
    elif isinstance(vector_str, np.array):
        return list(vector_str)
    else:
        print(f'{type(vector_str)=}')
        return vector_str


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
        # Convert string to vector
        "embedding": convert_vector(row["embedding"])
    }

# Batch insert documents into the collection


def batch_insert_documents(collection, documents, label=''):
    try:
        collection.insert_many(
            documents,
            vectors=[doc["embedding"] for doc in documents]
        )
    except Exception as err:
        print(f'Error on Chunk {k}')
        print(f'Error: {err}')

# Read CSV in chunks and upload to Astra DB


def upload_csv_to_astra(csv_file, ch_size=1000):
    # for k, chunk in tqdm(enumerate(pd.read_csv(csv_file, chunksize=ch_size))):
    for k, chunk in tqdm(enumerate(pd.read_csv(csv_file))):
        documents = [generate_document(row) for index, row in chunk.iterrows()]
        batch_insert_documents(collection, documents, label=k)
        # print(f"Inserted {len(documents)} documents")


# Path to the CSV file
csv_file_path = './csvfiles/wikidata_vectordb_datadump_10000_en.csv'

# Upload the CSV data to Astra DB
upload_csv_to_astra(csv_file_path)
