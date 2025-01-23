
from typing import List
from transformers import AutoModel, AutoTokenizer, AutoModelForSequenceClassification
import torch
from sqlalchemy import Column, Text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator
import json

engine = create_engine(f'sqlite:///../data/Wikidata/sqlite_cacheembeddings.db',
    pool_size=5,       # Limit the number of open connections
    max_overflow=10,   # Allow extra connections beyond pool_size
    pool_recycle=10  # Recycle connections every 10 seconds
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
    __tablename__ = 'embeddings'

    text = Column(Text, primary_key=True)
    embedding = Column(JSONType)

class JinaAIEmbedder:
    def __init__(self, passage_task="retrieval.passage", query_task="retrieval.query", embedding_dim=1024, cache=False):
        """
        Initializes the JinaAIEmbedder class with the model, tokenizer, and task identifiers.

        Parameters:
        - passage_task: Task identifier for embedding documents (default: "retrieval.passage").
        - query_task: Task identifier for embedding queries (default: "retrieval.query").
        - embedding_dim: The dimensionality of the embeddings (default: 1024).
        """
        self.passage_task = passage_task
        self.query_task = query_task
        self.embedding_dim = embedding_dim
        self.cache = cache

        self.model = AutoModel.from_pretrained("jinaai/jina-embeddings-v3", trust_remote_code=True)
        self.tokenizer = AutoTokenizer.from_pretrained("jinaai/jina-embeddings-v3", trust_remote_code=True)

    def _cache_embedding(self, text: str, embedding: List[float]):
        """Caches the text and its embedding in the SQLite database."""
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
        """Retrieves the embedding for the text from the cache, if available."""
        if self.cache:
            with Session() as session:
                cached = session.query(CacheEmbeddings).filter_by(text=text).first()
                if cached:
                    return cached.embedding
        return None

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Generates embeddings for a list of documents (passages), using the cache when possible.

        Parameters:
        - texts: A list of document strings to embed.

        Returns:
        - A list of embeddings, each corresponding to a document, with a dimensionality specified by embedding_dim.
        """
        # Separate cached and uncached texts
        cached_embeddings = []
        uncached_texts = []
        uncached_indices = []

        for idx, text in enumerate(texts):
            cached_embedding = self._get_cached_embedding(text)
            if cached_embedding:
                cached_embeddings.append((idx, cached_embedding))
            else:
                uncached_texts.append(text)
                uncached_indices.append(idx)

        # Embed uncached texts
        if uncached_texts:
            with torch.no_grad():
                new_embeddings = self.model.encode(uncached_texts, task=self.passage_task, truncate_dim=self.embedding_dim)
                for idx, text, embedding in zip(uncached_indices, uncached_texts, new_embeddings):
                    self._cache_embedding(text, embedding)  # Cache each new embedding
                    cached_embeddings.append((idx, embedding))

        # Sort cached embeddings back into the original order
        cached_embeddings.sort(key=lambda x: x[0])
        return [embedding for _, embedding in cached_embeddings]

    def embed_query(self, text: str) -> List[float]:
        """
        Generates an embedding for a single query, using the cache when possible.

        Parameters:
        - text: The query string to embed.

        Returns:
        - A single embedding as a list of floats with a dimensionality specified by embedding_dim.
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
        Initializes the JinaAIReranker class with the model.

        Parameters:
        - max_tokens: Size of the context window.
        """
        if max_tokens > 1024:
            raise ValueError("Max token should be less than or equal to 1024")

        self.max_tokens = max_tokens
        self.model = AutoModelForSequenceClassification.from_pretrained('jinaai/jina-reranker-v2-base-multilingual', trust_remote_code=True).to('cuda')

    def rank(self, query: str, texts: List[str]) -> List[float]:
        """
        Generates embeddings for a list of documents (passages).

        Parameters:
        - query: The user query to compare to.
        - texts: A list of document strings to rank.

        Returns:
        - A list of scores, each corresponding to a text, specifying the relevance to the query.
        """
        sentence_pairs = [[query, doc] for doc in texts]

        with torch.no_grad():
            return self.model.compute_score(sentence_pairs, max_length=self.max_tokens)

Base.metadata.create_all(engine)