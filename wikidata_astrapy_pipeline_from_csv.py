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


def vector_str_manipulation(vector_str):
    while '  ' in vector_str:
        vector_str = vector_str.replace('  ', ' ')

    vector_str = vector_str.replace(' ', ',')

    while ',,' in vector_str:
        vector_str = vector_str.replace(',,', ',')

    return vector_str.replace('[,', '[').replace(',]', ']')


def convert_vector(vector_str):
    # print(f'{vector_str=}')
    vector_str = vector_str_manipulation(vector_str)

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
        print(f'Error on Chunk {label}')
        print(f'Error: {err}')
        with open('deletme', 'a', newline='\n') as fdel:
            for doc in documents:
                try:
                    # Assign new UUID
                    doc["_id"] = str(uuid.uuid4())
                    collection.insert_one(
                        doc,
                        vector=doc["embedding"]
                    )
                except Exception as err2:
                    print(f'Inner error: {err2}')
                    fdel.write(f'{doc["embedding"]}\n')


# Read CSV in chunks and upload to Astra DB


def upload_csv_to_astra(csv_file=None, df=None, ch_size=1000):

    if csv_file is not None and df is None:
        iterator = enumerate(pd.read_csv(csv_file, chunksize=ch_size))
        for k, chunk in tqdm(iterator):
            documents = [
                generate_document(row) for index, row in chunk.iterrows()
            ]
            batch_insert_documents(collection, documents, label=k)
    elif df is not None:
        iterator = enumerate(pd.read_csv(csv_file, chunksize=ch_size))
        for k, row in tqdm(enumerate(df.iterrows())):
            documents = [generate_document(row[1])]
            batch_insert_documents(collection, documents, label=k)


# Path to the CSV file
csv_file_path = './csvfiles/wikidata_vectordb_datadump_10000_en.csv'

# print(f'Loading {csv_file_path}')
# df = pd.read_csv(csv_file_path)

# Clear deleteme file
with open('deletme', 'w', newline='\n') as fdel:
    fdel.write('')

# Upload the CSV data to Astra DB
upload_csv_to_astra(df=None, csv_file=csv_file_path, ch_size=1000)
