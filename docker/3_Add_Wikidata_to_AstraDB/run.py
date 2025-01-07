import sys
sys.path.append('../src')

from wikidataDB import Session, WikidataID, WikidataEntity
from wikidataEmbed import WikidataTextifier
from wikidataRetriever import AstraDBConnect

import json
from tqdm import tqdm
import os
import pickle
from datetime import datetime
import hashlib

MODEL = os.getenv("MODEL", "jina")
SAMPLE = os.getenv("SAMPLE", "false").lower() == "true"
EMBED_BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", 100))
QUERY_BATCH_SIZE = int(os.getenv("QUERY_BATCH_SIZE", 1000))
OFFSET = int(os.getenv("OFFSET", 0))
API_KEY_FILENAME = os.getenv("API_KEY", None)
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
LANGUAGE = os.getenv("LANGUAGE", 'en')
DUMPDATE = os.getenv("DUMPDATE", '09/18/2024')

# Load the Database
if not COLLECTION_NAME:
    raise ValueError("The COLLECTION_NAME environment variable is required")

if not API_KEY_FILENAME:
    API_KEY_FILENAME = os.listdir("../API_tokens")[0]
datastax_token = json.load(open(f"../API_tokens/{API_KEY_FILENAME}"))

textifier = WikidataTextifier(with_claim_aliases=False, with_property_aliases=False, language=LANGUAGE)
graph_store = AstraDBConnect(datastax_token, COLLECTION_NAME, model=MODEL, batch_size=EMBED_BATCH_SIZE)

# Load the Sample IDs
sample_ids = None
if SAMPLE:
    sample_ids = pickle.load(open("../data/Evaluation Data/Sample IDs (EN).pkl", "rb"))
    sample_ids = sample_ids[sample_ids['In Wikipedia']]

if __name__ == "__main__":
    with tqdm(total=9203786-OFFSET) as progressbar:
        with Session() as session:
            if SAMPLE:
                entities = session.query(WikidataEntity).offset(OFFSET).yield_per(QUERY_BATCH_SIZE)
            else:
                entities = session.query(WikidataEntity).join(WikidataID, WikidataEntity.id == WikidataID.id).filter(WikidataID.in_wikipedia == True).offset(OFFSET).yield_per(QUERY_BATCH_SIZE)
            doc_batch = []
            ids_batch = []

            for entity in entities:
                progressbar.update(1)
                if (not SAMPLE) or (entity.id in sample_ids['QID'].values):
                    chunks = textifier.chunk_text(entity, graph_store.tokenizer, max_length=graph_store.max_token_size)
                    for chunk_i in range(len(chunks)):
                        md5_hash = hashlib.md5(chunks[chunk_i].encode('utf-8')).hexdigest()
                        metadata={
                            # "MD5": md5_hash,
                            # "Claims": WikidataEntity.clean_claims_for_storage(entity.claims),
                            # "Label": entity.label,
                            # "Description": entity.description,
                            # "Aliases": entity.aliases,
                            "Date": datetime.now().isoformat(),
                            "QID": entity.id,
                            "ChunkID": chunk_i+1,
                            "Language": LANGUAGE,
                            "DumpDate": DUMPDATE
                        }
                        graph_store.add_document(id=f"{entity.id}_{LANGUAGE}_{chunk_i+1}", text=chunks[chunk_i], metadata=metadata)

                tqdm.write(progressbar.format_meter(progressbar.n, progressbar.total, progressbar.format_dict["elapsed"])) # tqdm is not wokring in docker compose. This is the alternative

            graph_store.push_batch()
