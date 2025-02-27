from langchain_astradb import AstraDBVectorStore
from langchain_core.documents import Document
from astrapy.info import CollectionVectorServiceOptions
from transformers import AutoTokenizer
import requests
from JinaAI import JinaAIEmbedder
import time
from elasticsearch import Elasticsearch

from mediawikiapi import MediaWikiAPI
from mediawikiapi.config import Config

class AstraDBConnect:
    def __init__(self, datastax_token, collection_name, model='nvidia', batch_size=8, cache_embeddings=False):
        """
        Initialize the AstraDBConnect object with the corresponding embedding model.

        Parameters:
        - datastax_token (dict): Credentials for DataStax Astra, including token and API endpoint.
        - collection_name (str): Name of the collection (table) where data is stored.
        - model (str): The embedding model to use ("nvidia" or "jina"). Default is 'nvidia'.
        - batch_size (int): Number of documents to accumulate before pushing to AstraDB. Default is 8.
        - cache_embeddings (bool): Whether to cache embeddings when using the Jina model. Default is False.
        """
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
            embeddings = JinaAIEmbedder(embedding_dim=1024, cache=cache_embeddings)
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
        """
        Add a single document to the internal batch for future storage.

        Parameters:
        - id (str): The unique identifier for the document (e.g., a QID).
        - text (str): The text content of the document.
        - metadata (dict): Additional metadata about the document.
        """
        doc = Document(page_content=text, metadata=metadata)
        self.doc_batch.append(doc)
        self.id_batch.append(id)

        # If we reach the batch size, push the accumulated documents to AstraDB
        if len(self.doc_batch) >= self.batch_size:
            self.push_batch()

    def push_batch(self):
        """
        Push the current batch of documents to AstraDB for storage.

        Retries automatically if a connection issue occurs, waiting for
        an active internet connection.
        """
        while True:
            try:
                self.graph_store.add_documents(self.doc_batch, ids=self.id_batch)
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

    def get_similar_qids(self, query, filter={}, K=50):
        """
        Retrieve similar QIDs for a given query string.

        Parameters:
        - query (str): The text query used to find similar documents.
        - filter (dict): Additional filtering criteria. Default is an empty dict.
        - K (int): Number of top results to return. Default is 50.

        Returns:
        - tuple: (list_of_qids, list_of_scores)
          where list_of_qids are the QIDs of the results and
          list_of_scores are the corresponding similarity scores.
        """
        while True:
            try:
                results = self.graph_store.similarity_search_with_relevance_scores(query, k=K, filter=filter)
                qid_results = [r[0].metadata['QID'] for r in results]
                score_results = [r[1] for r in results]
                return qid_results, score_results
            except Exception as e:
                print(e)
                while True:
                    try:
                        response = requests.get("https://www.google.com", timeout=5)
                        if response.status_code == 200:
                            break
                    except Exception as e:
                        time.sleep(5)

    def batch_retrieve_comparative(self, queries_batch, comparative_batch, K=50, Language=None):
        """
        Retrieve similar documents in a comparative fashion for each query and comparative item.

        Parameters:
        - queries_batch (pd.Series or list): Batch of query texts.
        - comparative_batch (pd.DataFrame): A dataframe where each column represents a comparative group.
        - K (int): Number of top results to return for each query. Default is 50.
        - Language (str or None): Optional language filter. Default is None. Only supports one language.

        Returns:
        - tuple: (list_of_qids, list_of_scores), each a list for each query.
        """
        qids = [[] for _ in range(len(queries_batch))]
        scores = [[] for _ in range(len(queries_batch))]

        for comp_col in comparative_batch.columns:
            for i in range(len(queries_batch)):
                filter = {'QID': comparative_batch[comp_col].iloc[i]}
                if (Language is not None) and (Language != ""):
                        filter['Language'] = Language

                result = self.get_similar_qids(queries_batch.iloc[i], filter=filter, K=K)
                qids[i] = qids[i] + result[0]
                scores[i] = scores[i] + result[1]

        return qids, scores

    def batch_retrieve(self, queries_batch, K=50, Language=None):
        """
        Retrieve similar documents for a batch of queries, with optional language filtering.

        Parameters:
        - queries_batch (pd.Series or list): Batch of query texts.
        - K (int): Number of top results to return. Default is 50.
        - Language (str or None): Comma-separated list of language codes or None.

        Returns:
        - tuple: (list_of_qids, list_of_scores)
        """
        filter = {}
        if (Language is not None) and (Language != ""):
            # Filter with an OR condition across multiple languages
            filter = {"$or": [{'Language': l} for l in Language.split(',')]}

        results = [
            self.get_similar_qids(queries_batch.iloc[i], K=K, filter=filter)
            for i in range(len(queries_batch))
        ]

        qids, scores = zip(*results)
        return list(qids), list(scores)

class WikidataKeywordSearch:
    def __init__(self, url, index_name = 'wikidata'):
        """
        Initialize the WikidataKeywordSearch object with an Elasticsearch instance.

        Parameters:
        - url (str): URL (host) of the Elasticsearch server.
        - index_name (str): Name of the Elasticsearch index. Default is 'wikidata'.
        """
        self.index_name = index_name
        self.es = Elasticsearch(url)

        # Create the index if it doesn't already exist
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
        """
        Perform a keyword-based search against the Elasticsearch index.

        Parameters:
        - query (str): The query string to match against document text.
        - K (int): Number of top results to return. Default is 50.

        Returns:
        - list: A list of raw Elasticsearch hits, each containing a '_score' and '_source'.
        """
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

    def get_similar_qids(self, query, filter_qid={}, K=50):
        """
        Retrieve QIDs based on a keyword-based search. Optionally filter by QID.

        Parameters:
        - query (str): The search string.
        - filter_qid (dict): Optional filter (currently unused, placeholder).
        - K (int): Number of top results to return. Default is 50.

        Returns:
        - tuple: (list_of_qids, list_of_scores)
        """
        results = self.search(query, K=K)
        qid_results = [r['_id'].split("_")[0] for r in results]
        score_results = [r['_score'] for r in results]
        return qid_results, score_results

    def batch_retrieve(self, queries_batch, K=50):
        """
        Perform keyword-based search for a batch of queries.

        Parameters:
        - queries_batch (pd.Series or list): A list or series of query strings.
        - K (int): Number of top results to return for each query. Default is 50.

        Returns:
        - tuple: (list_of_qid_lists, list_of_score_lists), each element corresponding to a single query.
        """
        results = [
            self.get_similar_qids(queries_batch.iloc[i], K=K)
            for i in range(len(queries_batch))
        ]

        qids, scores = zip(*results)
        return list(qids), list(scores)