import sys
sys.path.append('../src')

from wikidataRetriever import AstraDBConnect, WikidataKeywordSearch

import json
from tqdm import tqdm
import pandas as pd
import os
import pickle
import asyncio

MODEL = os.getenv("MODEL", "jina")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
API_KEY_FILENAME = os.getenv("API_KEY", None)
EVALUATION_PATH = os.getenv("EVALUATION_PATH")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

K = int(os.getenv("K", 50))
COMPARATIVE = os.getenv("COMPARATIVE", "false").lower() == "true"
COMPARATIVE_COLS = os.getenv("COMPARATIVE_COLS")
QUERY_COL = os.getenv("QUERY_COL")
QUERY_LANGUAGE = os.getenv("QUERY_LANGUAGE", 'en')
DB_LANGUAGE = os.getenv("DB_LANGUAGE", None)
RESTART = os.getenv("RESTART", "false").lower() == "true"
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
PREFIX = os.getenv("PREFIX", "")

OUTPUT_FILENAME = f"retrieval_results_{EVALUATION_PATH.split('/')[-2]}-{COLLECTION_NAME}-DB({DB_LANGUAGE})-Query({QUERY_LANGUAGE})"
# OUTPUT_FILENAME = f"retrieval_results_{EVALUATION_PATH.split('/')[-2]}-keyword-search-{LANGUAGE}"
if PREFIX != "":
    OUTPUT_FILENAME += PREFIX

# Load the Database
if not COLLECTION_NAME:
    raise ValueError("The COLLECTION_NAME environment variable is required")

if not API_KEY_FILENAME:
    API_KEY_FILENAME = os.listdir("../API_tokens")[0]
datastax_token = json.load(open(f"../API_tokens/{API_KEY_FILENAME}"))

graph_store = AstraDBConnect(datastax_token, COLLECTION_NAME, model=MODEL, batch_size=BATCH_SIZE)
# graph_store = WikidataKeywordSearch(ELASTICSEARCH_URL)

#Load the Evaluation Dataset
if not QUERY_COL:
    raise ValueError("The QUERY_COL environment variable is required")
if not EVALUATION_PATH:
    raise ValueError("The EVALUATION_PATH environment variable is required")

if not RESTART and os.path.exists(f"../data/Evaluation Data/{OUTPUT_FILENAME}.pkl"):
    print(f"Loading data from: {OUTPUT_FILENAME}")
    eval_data = pickle.load(open(f"../data/Evaluation Data/{OUTPUT_FILENAME}.pkl", "rb"))
else:
    eval_data = pickle.load(open(f"../data/Evaluation Data/{EVALUATION_PATH}", "rb"))

if 'Language' in eval_data.columns:
    eval_data = eval_data[eval_data['Language'] == QUERY_LANGUAGE]

if __name__ == "__main__":
    print(f"Running: {OUTPUT_FILENAME}")

    with tqdm(total=len(eval_data), disable=False) as progressbar:
        if 'Retrieval QIDs' not in eval_data:
            eval_data['Retrieval QIDs'] = None
        if 'Retrieval Score' not in eval_data:
            eval_data['Retrieval Score'] = None

        row_to_process = pd.isna(eval_data['Retrieval QIDs']) | pd.isna(eval_data['Retrieval Score'])
        progressbar.update((~row_to_process).sum())
        for i in range(0, row_to_process.sum(), BATCH_SIZE):
            batch_idx = eval_data[row_to_process].iloc[i:i+BATCH_SIZE].index
            batch = eval_data.loc[batch_idx]

            if COMPARATIVE:
                batch_results = asyncio.run(graph_store.batch_retrieve_comparative(batch[QUERY_COL], batch[COMPARATIVE_COLS.split(',')], K=K, Language=DB_LANGUAGE))
            else:
                batch_results = asyncio.run(graph_store.batch_retrieve(batch[QUERY_COL], K=K, Language=DB_LANGUAGE))

            eval_data.loc[batch_idx, 'Retrieval QIDs'] = pd.Series(batch_results[0]).values
            eval_data.loc[batch_idx, 'Retrieval Score'] = pd.Series(batch_results[1]).values

            progressbar.update(len(batch))
            tqdm.write(progressbar.format_meter(progressbar.n, progressbar.total, progressbar.format_dict["elapsed"])) # tqdm is not wokring in docker compose. This is the alternative
            if progressbar.n % 100 == 0:
                pickle.dump(eval_data, open(f"../data/Evaluation Data/{OUTPUT_FILENAME}.pkl", "wb"))
        pickle.dump(eval_data, open(f"../data/Evaluation Data/{OUTPUT_FILENAME}.pkl", "wb"))