import sys
sys.path.append('../src')

from wikidataDB import Session, WikidataID, WikidataEntity
from wikidataEmbed import WikidataTextifier, JinaAIEmbeddings

import json
from langchain_astradb import AstraDBVectorStore
from langchain_core.documents import Document
from astrapy.info import CollectionVectorServiceOptions
from transformers import AutoTokenizer
from tqdm import tqdm
from langchain_core.documents import Document
import requests
import time
import os
import pickle
import torch

NVIDIA = os.getenv("NVIDIA", "false").lower() == "true"
JINA = os.getenv("JINA", "false").lower() == "true"
SAMPLE = os.getenv("SAMPLE", "false").lower() == "true"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
OFFSET = int(os.getenv("OFFSET", 0))
API_KEY_FILENAME = os.getenv("API_KEY", None)
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

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

textifier = WikidataTextifier(with_claim_aliases=False, with_property_aliases=False)

graph_store = None
tokenizer = None
max_token_size = None
if NVIDIA:
    print("Using the Nvidia model")
    tokenizer = AutoTokenizer.from_pretrained('intfloat/e5-large-unsupervised', trust_remote_code=True, clean_up_tokenization_spaces=False)
    max_token_size = 500

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
    tokenizer = embeddings.tokenizer
    max_token_size = 1024

    graph_store = AstraDBVectorStore(
        collection_name=COLLECTION_NAME,
        embedding=embeddings,
        token=ASTRA_DB_APPLICATION_TOKEN,
        api_endpoint=ASTRA_DB_API_ENDPOINT,
        namespace=ASTRA_DB_KEYSPACE,
    )

# Load the Sample IDs
sample_ids = None
if SAMPLE:
    sample_ids = pickle.load(open("../data/Evaluation Data/Sample IDs (EN).pkl", "rb"))
    sample_ids = sample_ids[sample_ids['In Wikipedia']]

if __name__ == "__main__":
    with tqdm(total=9203786) as progressbar:
        with Session() as session:
            entities = session.query(WikidataEntity).join(WikidataID, WikidataEntity.id == WikidataID.id).filter(WikidataID.in_wikipedia == True).offset(OFFSET).yield_per(BATCH_SIZE)
            progressbar.update(OFFSET)
            doc_batch = []
            ids_batch = []

            for entity in entities:
                progressbar.update(1)
                if SAMPLE and (entity.id in sample_ids['QID'].values):
                    chunks = textifier.chunk_text(entity, tokenizer, max_length=max_token_size)
                    for chunk_i in range(len(chunks)):
                        doc = Document(page_content=chunks[chunk_i], metadata={"QID": entity.id, "ChunkID": chunk_i+1})
                        doc_batch.append(doc)
                        ids_batch.append(f"{entity.id}_{chunk_i+1}")

                        if len(doc_batch) >= BATCH_SIZE:
                            tqdm.write(progressbar.format_meter(progressbar.n, progressbar.total, progressbar.format_dict["elapsed"])) # tqdm is not wokring in docker compose. This is the alternative
                            try:
                                graph_store.add_documents(doc_batch, ids=ids_batch)
                                torch.cuda.empty_cache()
                                doc_batch = []
                                ids_batch = []
                            except Exception as e:
                                print(e)
                                while True:
                                    try:
                                        response = requests.get("https://www.google.com", timeout=5)
                                        if response.status_code == 200:
                                            break
                                    except Exception as e:
                                        print("Waiting for internet connection...")
                                        time.sleep(5)

            if len(doc_batch) > 0:
                graph_store.add_documents(doc_batch, ids=ids_batch)