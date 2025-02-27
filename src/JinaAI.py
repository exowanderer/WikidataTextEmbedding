
from typing import List
from transformers import AutoModel, AutoTokenizer, AutoModelForSequenceClassification
import torch
from sqlalchemy import Column, Text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator
import json
import requests
import numpy as np
import base64

"""
SQLite database setup for caching the query embeddings for a faster evaluation process.
"""
engine = create_engine(
    'sqlite:///../data/Wikidata/sqlite_cacheembeddings.db',
    pool_size=5,       # Limit the number of open connections
    max_overflow=10,   # Allow extra connections beyond pool_size
    pool_recycle=10    # Recycle connections every 10 seconds
)

Base = declarative_base()
Session = sessionmaker(bind=engine)

class JSONType(TypeDecorator):
    """Custom SQLAlchemy type for JSON storage in SQLite."""
    impl = Text

    def process_bind_param(self, value, dialect):
        if value is not None:
            return json.dumps(value, separators=(',', ':'))
        return None

    def process_result_value(self, value, dialect):
        if value is not None:
            return json.loads(value)
        return None

class CacheEmbeddings(Base):
    """Represents a cache entry for a text string and its embedding."""
    __tablename__ = 'embeddings'

    text = Column(Text, primary_key=True)
    embedding = Column(JSONType)

class JinaAIEmbedder:
    def __init__(self, passage_task="retrieval.passage", query_task="retrieval.query", embedding_dim=1024, cache=False, api_key_path="../API_tokens/jina_api.json"):
        """
        Initializes the JinaAIEmbedder class with the model, tokenizer, and task identifiers.

        Parameters:
        - passage_task (str): Task identifier for embedding documents. Defaults to "retrieval.passage".
        - query_task (str): Task identifier for embedding queries. Defaults to "retrieval.query".
        - embedding_dim (int): Dimensionality of the embeddings. Defaults to 1024.
        - cache (bool): Whether to cache query embeddings in the database. Defaults to False.
        - api_key_path (str): Path to the JSON file containing the Jina API key. Defaults to "../API_tokens/jina_api.json".
        """
        self.passage_task = passage_task
        self.query_task = query_task
        self.embedding_dim = embedding_dim
        self.cache = cache

        self.model = AutoModel.from_pretrained("jinaai/jina-embeddings-v3", trust_remote_code=True).to('cuda')
        self.tokenizer = AutoTokenizer.from_pretrained("jinaai/jina-embeddings-v3", trust_remote_code=True)

        self.api_key = json.load(open(api_key_path, 'r+'))['API_KEY']

    def _cache_embedding(self, text: str, embedding: List[float]):
        """
        Caches the text and its embedding in the SQLite database.

        Parameters:
        - text (str): The text string.
        - embedding (List[float]): The embedding vector for the text.
        """
        if self.cache:
            embedding = embedding.tolist()
            with Session() as session:
                try:
                    cached = CacheEmbeddings(text=text, embedding=embedding)
                    session.merge(cached)
                    session.commit()
                except Exception as e:
                    session.rollback()
                    raise e

    def _get_cached_embedding(self, text: str) -> List[float]:
        """
        Retrieves a previously cached embedding for the specified text.

        Parameters:
        - text (str): The text string.

        Returns:
        - List[float] or None: The embedding if found in cache, otherwise None.
        """
        if self.cache:
            with Session() as session:
                cached = session.query(CacheEmbeddings).filter_by(text=text).first()
                if cached:
                    return cached.embedding
        return None

    def api_embed(self, text, task="retrieval.query"):
        """
        Generates an embedding for the given text using the Jina Embeddings API.

        Parameters:
        - text (str): The text to embed.
        - task (str): The task identifier (e.g., "retrieval.query" or "retrieval.passage").

        Returns:
        - np.ndarray: The resulting embedding vector as a NumPy array.
        """
        url = 'https://api.jina.ai/v1/embeddings'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}'
        }

        data = {
            "model": "jina-embeddings-v3",
            "dimensions": self.embedding_dim,
            "embedding_type": "base64",
            "task": task,
            "late_chunking": False,
            "input": [
                text
            ]
        }

        response = requests.post(url, headers=headers, json=data)
        binary_data = base64.b64decode(response.json()['data'][0]['embedding'])
        embedding_array = np.frombuffer(binary_data, dtype='<f4')
        return embedding_array

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Generates embeddings for a list of document (passage) texts.

        Caching is not used here by default to avoid storing large numbers of document embeddings.

        Parameters:
        - texts (List[str]): A list of document texts to embed.

        Returns:
        - List[List[float]]: A list of embedding vectors, each corresponding to a document.
        """
        with torch.no_grad():
            embeddings = self.model.encode(texts, task=self.passage_task, truncate_dim=self.embedding_dim)
        return embeddings

    def embed_query(self, text: str) -> List[float]:
        """
        Generates an embedding for a single query string, optionally using and updating the cache.

        Parameters:
        - text (str): The query text to embed.

        Returns:
        - List[float]: The embedding vector corresponding to the query.
        """
        cached_embedding = self._get_cached_embedding(text)
        if cached_embedding:
            return cached_embedding

        with torch.no_grad():
            embedding = self.model.encode([text], task=self.query_task, truncate_dim=self.embedding_dim)[0]
            self._cache_embedding(text, embedding)
            return embedding

class JinaAIReranker:
    def __init__(self, max_tokens=1024):
        """
        Initializes the JinaAIReranker with a maximum token length and the Jina Reranker model.

        Parameters:
        - max_tokens (int): Maximum sequence length for the reranker (must be <= 1024).

        Raises:
        - ValueError: If max_tokens is greater than 1024.
        """
        if max_tokens > 1024:
            raise ValueError("Max token should be less than or equal to 1024")

        self.max_tokens = max_tokens
        self.model = AutoModelForSequenceClassification.from_pretrained('jinaai/jina-reranker-v2-base-multilingual', trust_remote_code=True).to('cuda')

    def rank(self, query: str, texts: List[str]) -> List[float]:
        """
        Scores a list of documents based on their relevance to the given query.

        Parameters:
        - query (str): The user's query text.
        - texts (List[str]): A list of document texts to rank.

        Returns:
        - List[float]: A list of relevance scores, each corresponding to one document.
        """
        sentence_pairs = [[query, doc] for doc in texts]

        with torch.no_grad():
            return self.model.compute_score(sentence_pairs, max_length=self.max_tokens)

# Create tables if they don't already exist.
Base.metadata.create_all(engine)