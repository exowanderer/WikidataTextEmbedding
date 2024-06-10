import ast
import astrapy
import numpy as np
import os
import pandas as pd
import sys
import uuid

from tqdm import tqdm


def is_docker():
    """Check if the script is running inside a Docker container."""
    # Check for .dockerenv file
    if os.path.exists('/.dockerenv'):
        return True

    # Check for Docker-specific entries in /proc/1/cgroup
    try:
        with open('/proc/1/cgroup', 'rt') as f:
            for line in f:
                if 'docker' in line:
                    return True
    except Exception:
        pass

    return False


def vector_str_manipulation(vector_str):
    # Function to convert vector string to list of floats
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


def generate_statement_document(row):
    return {
        "_id": row["uuid"] if "uuid" in row else str(uuid.uuid4()),
        "qid": row["qid"],
        "pid": row["pid"],
        "value": row["value"],
        "item_label": row["item_label"],
        "property_label": row["property_label"],
        "value_content": row["value_content"],
        "statement": row["statement"],
        # Convert string to vector
        "embedding": convert_vector(embedding)
    }


def generate_item_document(row):
    return {
        "_id": row["uuid"] if "uuid" in row else str(uuid.uuid4()),
        "qid": row["qid"],
        "chunk_id": row["chunk_id"],
        "qid_chunk": row["qid_chunk"],
        "n_statements": row["n_statements"],
        "n_sitelinks": row["n_sitelinks"],
        "n_descriptions": row["n_descriptions"],
        "n_lines": row["n_lines"],
        "item_str": row["item_str"],
        # Convert string to vector
        "embedding": convert_vector(row["embedding"])
    }


def generate_document(row, pipeline='item'):
    if pipeline == 'item':
        return generate_item_document(row)
    if pipeline == 'statement':
        return generate_statement_document(row)

# Batch insert documents into the collection


def batch_insert_documents(collection, documents, label=''):
    embeddings = [doc["embedding"] for doc in documents]
    documents = [
        {key: val}
        for doc in documents
        for key, val in doc.items()
        if key != 'embedding'
    ]

    try:
        collection.insert_many(documents, vectors=embeddings)
    except Exception as err:
        # TODO: introduce recursive looking
        # batch_insert_documents(collection, documents, label=label)
        print(f'Error on Chunk {label}')
        print(f'Error: {err}')
        uuid_err_counter = 0

        for embedding_, doc in tqdm(zip(embeddings, documents)):
            try:
                collection.insert_one(doc, vector=embedding_)
            except Exception as err2:
                uuid_err = "Failed to insert document with _id"
                # uuid_err = "Document already exists with the given _id"

                if uuid_err not in str(err2):
                    print(f'Inner error: {err2}')
                else:
                    uuid_err_counter = uuid_err_counter + 1

            print(f'Number of UUID already exists errors: {uuid_err_counter}')


# Read CSV in chunks and upload to Astra DB


def upload_csv_to_astra(
        collection, csv_file=None, df=None, ch_size=100, pipeline='item'):

    if csv_file is not None and df is None:
        iterator = enumerate(pd.read_csv(csv_file, chunksize=ch_size))
        for k, chunk in tqdm(iterator):
            documents = [
                generate_document(row, pipeline=pipeline)
                for index, row in chunk.iterrows()
            ]
            batch_insert_documents(collection, documents, label=k)
    elif df is not None:
        iterator = enumerate(pd.read_csv(csv_file, chunksize=ch_size))
        for k, row in tqdm(df.iterrows()):
            documents = [generate_document(row)]
            batch_insert_documents(collection, documents, label=k)


def confirm_drop_collection(collection):
    print(f'Collection exists: {collection}')
    confirm = input("Confirm name of Collection to dropping: ")

    while True:

        if confirm.upper() == collection.upper():
            print(f"\nDropping Collection {collection}\n")
            return True

        print('Input not confirm. Ending pipeline.')
        sys.exit()


if __name__ == '__main__':
    # Initialize the DataStax Astra client
    from argparse import ArgumentParser
    args = ArgumentParser('Astrapy Pipeline for Wikidata Embeddings')
    args.add_argument('--pipeline', '-p', type=str, default='item')
    args.add_argument('--chunksize', '-c', type=int, default=100)
    args.add_argument('--collection', type=str, default='wikidata')
    args.add_argument('--embed_dim', '-ed', type=int, default=768)
    args.add_argument('--restart_collection', '-rc', action='store_true')
    args = args.parse_args()

    PIPELINE = os.environ.get('PIPELINE', args.pipeline)
    CHUNKSIZE = os.environ.get('CHUNKSIZE', args.chunksize)
    COLLECTION_NAME = os.environ.get('COLLECTION_NAME', args.collection)
    EMBED_DIM = os.environ.get('EMBED_DIM', args.embed_dim)
    RESTART_COLLECTION = os.environ.get(
        'RESTART_COLLECTION',
        args.restart_collection
    )

    IS_DOCKER = is_docker()

    # TODO: Check if this is irrelevant
    CHUNKSIZE = args.pipeline if CHUNKSIZE is None else int(CHUNKSIZE)

    if COLLECTION_NAME is None:
        COLLECTION_NAME = args.collection

    api_url = os.environ.get('ASTRACS_API_URL')
    api_token = os.environ.get('ASTRACS_API_KEY')

    client = astrapy.DataAPIClient(api_token)
    database = client.get_database_by_api_endpoint(api_url)

    db_exists = COLLECTION_NAME in database.list_collection_names()

    if db_exists and RESTART_COLLECTION:
        is_confirmed = confirm_drop_collection(COLLECTION_NAME)
        if is_confirmed:
            print(f'Dropping collection {COLLECTION_NAME}')
            result = db.drop_collection(name_or_collection=COLLECTOIN)
            print(f'{result=}')

    if not db_exists:
        database.create_collection(
            COLLECTION_NAME,
            dimension=EMBED_DIM,
            metric=astrapy.constants.VectorMetric.COSINE,
            indexing={"deny":  ["very_long_text"]}
        )

    collection = database.get_collection(COLLECTION_NAME)  # "testwikidata"

    # Path to the CSV file
    filename = 'wikidata_vectordb_datadump_item_chunks_1000000_en.csv'
    csv_file_path = f'./csvfiles/{filename}'

    if IS_DOCKER:
        csv_file_path = csv_file_path.replace('./', '/app')

    # print(f'Loading {csv_file_path}')
    # df = pd.read_csv(csv_file_path)

    # Upload the CSV data to Astra DB
    upload_csv_to_astra(
        collection=collection,
        df=None,
        csv_file=csv_file_path,
        ch_size=CHUNKSIZE,
        pipeline=PIPELINE,
    )
