from langchain_astradb import AstraDBVectorStore
from langchain_core.documents import Document
from astrapy.info import CollectionVectorServiceOptions
from transformers import AutoTokenizer
import torch
import requests
import time
from wikidataEmbed import JinaAIEmbeddings
import asyncio
from elasticsearch import Elasticsearch

from mediawikiapi import MediaWikiAPI
from mediawikiapi.config import Config

class AstraDBConnect:
    def __init__(self, datastax_token, collection_name, model='nvidia', batch_size=8):
        ASTRA_DB_DATABASE_ID = datastax_token['ASTRA_DB_DATABASE_ID']
        ASTRA_DB_APPLICATION_TOKEN = datastax_token['ASTRA_DB_APPLICATION_TOKEN']
        ASTRA_DB_API_ENDPOINT = datastax_token["ASTRA_DB_API_ENDPOINT"]
        ASTRA_DB_KEYSPACE = datastax_token["ASTRA_DB_KEYSPACE"]

        self.batch_size = batch_size
        self.model = model
        self.collection_name = collection_name
        self.doc_batch = []
        self.id_batch = []

        if model == 'nvidia':
            self.tokenizer = AutoTokenizer.from_pretrained('intfloat/e5-large-unsupervised', trust_remote_code=True, clean_up_tokenization_spaces=False)
            self.max_token_size = 500

            collection_vector_service_options = CollectionVectorServiceOptions(
                provider="nvidia",
                model_name="NV-Embed-QA"
            )

            self.graph_store = AstraDBVectorStore(
                collection_name=collection_name,
                collection_vector_service_options=collection_vector_service_options,
                token=ASTRA_DB_APPLICATION_TOKEN,
                api_endpoint=ASTRA_DB_API_ENDPOINT,
                namespace=ASTRA_DB_KEYSPACE,
            )
        elif model == 'jina':
            embeddings = JinaAIEmbeddings(embedding_dim=1024)
            self.tokenizer = embeddings.tokenizer
            self.max_token_size = 1024

            self.graph_store = AstraDBVectorStore(
                collection_name=collection_name,
                embedding=embeddings,
                token=ASTRA_DB_APPLICATION_TOKEN,
                api_endpoint=ASTRA_DB_API_ENDPOINT,
                namespace=ASTRA_DB_KEYSPACE,
            )
        else:
            raise "Invalid model"

    def add_document(self, id, text, metadata):
        doc = Document(page_content=text, metadata=metadata)
        self.doc_batch.append(doc)
        self.id_batch.append(id)

        if len(self.doc_batch) >= self.batch_size:
            self.push_batch()

    def push_batch(self):
        while True:
            try:
                self.graph_store.add_documents(self.doc_batch, ids=self.id_batch)
                torch.cuda.empty_cache()
                self.doc_batch = []
                self.id_batch = []
                break
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

    async def get_similar_qids_async(self, query, filter_qid={}, K=50):
        while True:
            try:
                results = self.graph_store.similarity_search_with_relevance_scores(query, k=K, filter=filter_qid)
                qid_results = [r[0].metadata['QID'] for r in results]
                score_results = [r[1] for r in results]
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
                        asyncio.sleep(5)

    async def batch_retrieve_comparative(self, queries_batch, comparative_batch, K=50):
        qids = [[] for _ in range(len(queries_batch))]
        scores = [[] for _ in range(len(queries_batch))]

        for comp_col in comparative_batch.columns:
            tasks = [
                self.get_similar_qids_async(queries_batch.iloc[i], filter_qid={'QID': comparative_batch[comp_col].iloc[i]}, K=K)
                for i in range(len(queries_batch))
            ]
            results = await asyncio.gather(*tasks)

            for i, (temp_qid, temp_score) in enumerate(results):
                qids[i] = qids[i] + temp_qid
                scores[i] = scores[i] + temp_score
        return qids, scores

    async def batch_retrieve(self, queries_batch, K=50):
        tasks = [
            self.get_similar_qids_async(queries_batch.iloc[i], K=K)
            for i in range(len(queries_batch))
        ]
        results = await asyncio.gather(*tasks)

        qids, scores = zip(*results)
        return list(qids), list(scores)

class WikidataKeywordSearch:
    def __init__(self, url, index_name = 'wikidata'):
        self.index_name = index_name
        self.es = Elasticsearch(url)
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, body={
            "mappings": {
                "properties": {
                    "text": {
                        "type": "text"
                    }
                }
            }
        })

    def search(self, query, K=50):
        search_body = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "text": {
                                    "query": query,
                                    "operator": "or",
                                    "boost": 1.0
                                }
                            }
                        },
                        {
                            "match_all": {
                                "boost": 0.01  # Lower boost to make match_all results less relevant
                            }
                        }
                    ]
                }
            },
            "size": K,
            "sort": [
                {
                    "_score": {
                        "order": "desc"
                    }
                }
            ]
        }
        response = self.es.search(index=self.index_name, body=search_body)
        return [hit for hit in response['hits']['hits']]


    async def get_similar_qids_async(self, query, filter_qid={}, K=50):
        while True:
            try:
                results = self.search(query, K=K)
                qid_results = [r['_id'].split("_")[0] for r in results]
                score_results = [r['_score'] for r in results]
                return qid_results, score_results
            except Exception as e:
                print(e)
                while True:
                    try:
                        response = requests.get("https://www.google.com", timeout=5)
                        if response.status_code == 200:
                            break
                    except Exception as e:
                        asyncio.sleep(5)

    async def batch_retrieve(self, queries_batch, K=50):
        tasks = [
            self.get_similar_qids_async(queries_batch.iloc[i], K=K)
            for i in range(len(queries_batch))
        ]
        results = await asyncio.gather(*tasks)

        qids, scores = zip(*results)
        return list(qids), list(scores)