
from typing import List
from transformers import AutoModel, AutoTokenizer, AutoModelForSequenceClassification
import torch

class JinaAIEmbedder:
    def __init__(self, passage_task="retrieval.passage", query_task="retrieval.query", embedding_dim=1024):
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

        self.model = AutoModel.from_pretrained("jinaai/jina-embeddings-v3", trust_remote_code=True).to('cuda')
        self.tokenizer = AutoTokenizer.from_pretrained("jinaai/jina-embeddings-v3", trust_remote_code=True)

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Generates embeddings for a list of documents (passages).

        Parameters:
        - texts: A list of document strings to embed.

        Returns:
        - A list of embeddings, each corresponding to a document, with a dimensionality specified by embedding_dim.
        """
        with torch.no_grad():
            return self.model.encode(texts, task=self.passage_task, truncate_dim=self.embedding_dim)

    def embed_query(self, text: str) -> List[float]:
        """
        Generates an embedding for a single query.

        Parameters:
        - query: The query string to embed.

        Returns:
        - A single embedding as a list of floats with a dimensionality specified by embedding_dim.
        """
        with torch.no_grad():
            return self.model.encode([text], task=self.query_task, truncate_dim=self.embedding_dim)[0]

class JinaAIReranker:
    def __init__(self, max_tokens=1024):
        """
        Initializes the JinaAIReranker class with the model.

        Parameters:
        - max_tokens: Size of the context window.
        """
        if max_tokens > 1024:
            raise "Max token should be less than or equal to 1024"

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