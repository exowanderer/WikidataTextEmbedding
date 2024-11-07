import sys
sys.path.append('../src')

from wikidataEmbed import WikidataTextifier, JinaAIEmbeddings

import json
from langchain_astradb import AstraDBVectorStore
from langchain_core.documents import Document
from astrapy.info import CollectionVectorServiceOptions
from transformers import AutoTokenizer
from tqdm import tqdm
from langchain_core.documents import Document
import requests
import pandas as pd
import os
import pickle
import torch
import asyncio

NVIDIA = os.getenv("NVIDIA", "false").lower() == "true"
JINA = os.getenv("JINA", "false").lower() == "true"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
API_KEY_FILENAME = os.getenv("API_KEY", None)
EVALUATION_PATH = os.getenv("EVALUATION_PATH")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

K = int(os.getenv("K", 50))
COMPARATIVE = os.getenv("COMPARATIVE", "false").lower() == "true"
COMPARATIVE_COLS = os.getenv("COMPARATIVE_COLS")
QUERY_COL = os.getenv("QUERY_COL")
LANGUAGE = os.getenv("LANGUAGE", 'en')
RESTART = os.getenv("RESTART", "false").lower() == "true"


OUTPUT_FILENAME = f"retrieval_results_{EVALUATION_PATH.split('/')[-2]}-{COLLECTION_NAME}-{LANGUAGE}"

# Load the Database
if not COLLECTION_NAME:
    raise ValueError("The COLLECTION_NAME environment variable is required")

if not API_KEY_FILENAME:
    API_KEY_FILENAME = os.listdir("../API_tokens")[0]
datastax_token = json.load(open(f"../API_tokens/{API_KEY_FILENAME}"))
ASTRA_DB_DATABASE_ID = datastax_token['ASTRA_DB_DATABASE_ID']
ASTRA_DB_APPLICATION_TOKEN = datastax_token['ASTRA_DB_APPLICATION_TOKEN']
ASTRA_DB_API_ENDPOINT = datastax_token["ASTRA_DB_API_ENDPOINT"]
ASTRA_DB_KEYSPACE = datastax_token["ASTRA_DB_KEYSPACE"]

graph_store = None
tokenizer = None
max_token_size = None
if NVIDIA:
    print("Using the Nvidia model")
    collection_vector_service_options = CollectionVectorServiceOptions(
        provider="nvidia",
        model_name="NV-Embed-QA"
    )

    graph_store = AstraDBVectorStore(
        collection_name=COLLECTION_NAME,
        collection_vector_service_options=collection_vector_service_options,
        token=ASTRA_DB_APPLICATION_TOKEN,
        api_endpoint=ASTRA_DB_API_ENDPOINT,
        namespace=ASTRA_DB_KEYSPACE,
    )
else:
    print("Using the Jina model")
    embeddings = JinaAIEmbeddings(embedding_dim=1024)

    graph_store = AstraDBVectorStore(
        collection_name=COLLECTION_NAME,
        embedding=embeddings,
        token=ASTRA_DB_APPLICATION_TOKEN,
        api_endpoint=ASTRA_DB_API_ENDPOINT,
        namespace=ASTRA_DB_KEYSPACE,
    )

#Load the Evaluation Dataset
if not QUERY_COL:
    raise ValueError("The QUERY_COL environment variable is required")
if not EVALUATION_PATH:
    raise ValueError("The EVALUATION_PATH environment variable is required")

if not RESTART and os.path.exists(f"../data/Evaluation Data/{OUTPUT_FILENAME}.pkl"):
    print("Loading data...")
    eval_data = pickle.load(open(f"../data/Evaluation Data/{OUTPUT_FILENAME}.pkl", "rb"))
else:
    eval_data = pickle.load(open(f"../data/Evaluation Data/{EVALUATION_PATH}", "rb"))

if 'Language' in eval_data.columns:
    eval_data = eval_data[eval_data['Language'] == LANGUAGE]

async def get_similar_qids_async(query, filter_qids=[{}]):
    # Async function to retrieve similar QIDs
    while True:
        try:
            qid_results = []
            score_results = []
            for filter_qid in filter_qids:
                results = graph_store.similarity_search_with_relevance_scores(query, k=K, filter=filter_qid)
                qid_results.extend([r[0].metadata['QID'] for r in results])
                score_results.extend([r[1] for r in results])
            torch.cuda.empty_cache()
            return qid_results, score_results
        except Exception as e:
            print(e)
            while True:
                try:
                    response = requests.get("https://www.google.com", timeout=5)
                    if response.status_code == 200:
                        break
                except Exception as e:
                    print("Waiting for internet connection...")
                    asyncio.sleep(5)

async def retrieve_qids_for_batch(queries_batch):
    # Function to retrieve results for each batch asynchronously
    tasks = []
    for i, query in queries_batch.iterrows():
        filter_qids = [{}]
        if COMPARATIVE:
            filter_qids = [{'QID': query[col]} for col in COMPARATIVE_COLS.split(',')]
        tasks.append(get_similar_qids_async(query[QUERY_COL], filter_qids=filter_qids))
    results = await asyncio.gather(*tasks)
    return [r[0] for r in results], [r[1] for r in results]

if __name__ == "__main__":
    with tqdm(total=len(eval_data)) as progressbar:
        if 'Retrieval QIDs' not in eval_data:
            eval_data['Retrieval QIDs'] = None
        if 'Retrieval Score' not in eval_data:
            eval_data['Retrieval Score'] = None

        row_to_process = pd.isna(eval_data['Retrieval QIDs']) | pd.isna(eval_data['Retrieval Score'])
        progressbar.update((~row_to_process).sum())
        for i in range(0, row_to_process.sum(), BATCH_SIZE):
            batch_idx = eval_data[row_to_process].iloc[i:i+BATCH_SIZE].index
            batch = eval_data.loc[batch_idx]

            batch_results = asyncio.run(retrieve_qids_for_batch(batch))
            eval_data.loc[batch_idx, 'Retrieval QIDs'] = pd.Series(batch_results[0]).values
            eval_data.loc[batch_idx, 'Retrieval Score'] = pd.Series(batch_results[1]).values

            progressbar.update(len(batch))
            tqdm.write(progressbar.format_meter(progressbar.n, progressbar.total, progressbar.format_dict["elapsed"])) # tqdm is not wokring in docker compose. This is the alternative
            pickle.dump(eval_data, open(f"../data/Evaluation Data/{OUTPUT_FILENAME}.pkl", "wb"))