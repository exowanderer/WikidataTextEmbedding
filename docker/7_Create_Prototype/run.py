# TODO: package with setup inside docker to avoid sys.path mods
import sys
sys.path.append('../src')

from wikidataEmbed import WikidataTextifier
from wikidataRetriever import AstraDBConnect
from datasets import load_dataset
from multiprocessing import Process, Queue, Manager

import json
from tqdm import tqdm
import os
from datetime import datetime
import hashlib
from types import SimpleNamespace
import time

MODEL = os.getenv("MODEL", "jinaapi")
NUM_PROCESSES = int(os.getenv("NUM_PROCESSES", 4))
EMBED_BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", 100))

QUEUE_SIZE = 2 * EMBED_BATCH_SIZE * NUM_PROCESSES  # enough to not run out
QUEUE_SIZE = int(os.getenv("QUEUE_SIZE", QUEUE_SIZE))

DB_API_KEY_FILENAME = os.getenv("DB_API_KEY", "datastax_wikidata.json")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

CHUNK_NUM = os.getenv("CHUNK_NUM")

assert(CHUNK_NUM is not None), \
    "Please provide `CHUNK_NUM` env var at docker run"

LANGUAGE = "en"
TEXTIFIER_LANGUAGE = "en"
DUMPDATE = "09/18/2024"

# Load the Database
if not COLLECTION_NAME:
    raise ValueError("The COLLECTION_NAME environment variable is required")

if not TEXTIFIER_LANGUAGE:
    TEXTIFIER_LANGUAGE = LANGUAGE

FILEPATH = f"../data/Wikidata/chunks/chunk_{CHUNK_NUM}.json.gz"

# TODO: Push this dict into a json
chunk_sizes = {"chunk_0":992458,"chunk_1":802125,"chunk_2":589652,"chunk_3":310440,"chunk_4":43477,"chunk_5":156867,"chunk_6":141965,"chunk_7":74047,"chunk_8":27104,"chunk_9":70759,"chunk_10":71395,"chunk_11":186698,"chunk_12":153182,"chunk_13":137155,"chunk_14":929827,"chunk_15":853027,"chunk_16":571543,"chunk_17":335565,"chunk_18":47264,"chunk_19":135986,"chunk_20":160411,"chunk_21":76377,"chunk_22":26321,"chunk_23":70572,"chunk_24":68613,"chunk_25":179806,"chunk_26":159587,"chunk_27":139912,"chunk_28":876104,"chunk_29":864360,"chunk_30":590603,"chunk_31":358747,"chunk_32":47772,"chunk_33":135633,"chunk_34":159629,"chunk_35":81231,"chunk_36":24912,"chunk_37":69201,"chunk_38":67131,"chunk_39":172234,"chunk_40":167698,"chunk_41":142276,"chunk_42":821175,"chunk_43":892005,"chunk_44":600584,"chunk_45":374793,"chunk_46":47443,"chunk_47":134784,"chunk_48":155247,"chunk_49":86997,"chunk_50":24829,"chunk_51":68053,"chunk_52":63517,"chunk_53":167660,"chunk_54":175827,"chunk_55":142816,"chunk_56":765400,"chunk_57":900655,"chunk_58":628866,"chunk_59":396886,"chunk_60":46907,"chunk_61":135384,"chunk_62":154864,"chunk_63":88112,"chunk_64":23353,"chunk_65":67446,"chunk_66":40301,"chunk_67":176420,"chunk_68":183715,"chunk_69":149547,"chunk_70":713006,"chunk_71":901222,"chunk_72":652770,"chunk_73":419554,"chunk_74":52246,"chunk_75":134064,"chunk_76":153318,"chunk_77":92710,"chunk_78":22790,"chunk_79":66521,"chunk_80":34397,"chunk_81":173357,"chunk_82":186788,"chunk_83":153870,"chunk_84":657926,"chunk_85":902477,"chunk_86":655319,"chunk_87":455111,"chunk_88":69724,"chunk_89":133629,"chunk_90":146534,"chunk_91":101890,"chunk_92":21324,"chunk_93":65448,"chunk_94":33345,"chunk_95":162191,"chunk_96":192226,"chunk_97":159451,"chunk_98":598037,"chunk_99":903618,"chunk_100":662580,"chunk_101":484690,"chunk_102":86616,"chunk_103":135160,"chunk_104":106630,"chunk_105":142249,"chunk_106":19290,"chunk_107":60073,"chunk_108":39131,"chunk_109":155251,"chunk_110":190337,"chunk_111":166210,"chunk_112":26375}

total_entities = chunk_sizes[f"chunk_{CHUNK_NUM}"]

datastax_token = json.load(open(f"../API_tokens/{DB_API_KEY_FILENAME}"))
dataset = load_dataset(
    "philippesaade/wikidata",
    data_files=f"data/chunk_{CHUNK_NUM}-*.parquet",
    streaming=True,
    split="train"
)

def process_items(queue, progress_bar):
    """Worker function that processes items from the queue and adds them to AstraDB."""
    datastax_token = json.load(open(f"../API_tokens/{DB_API_KEY_FILENAME}"))
    graph_store = AstraDBConnect(
        datastax_token,
        COLLECTION_NAME,
        model=MODEL,
        batch_size=EMBED_BATCH_SIZE,
        cache_embeddings="wikidata_prototype"
    )
    textifier = WikidataTextifier(
        language=LANGUAGE,
        langvar_filename=TEXTIFIER_LANGUAGE
    )

    while True:
        item = queue.get()
        if item is None:
            break  # Exit condition for worker processes

        item_id = item['id']
        item_label = textifier.get_label(item_id, json.loads(item['labels']))
        item_description = textifier.get_description(
            item_id,
            json.loads(item['descriptions'])
        )
        item_aliases = textifier.get_aliases(json.loads(item['aliases']))

        if item_label is not None:
            # TODO: Verify: If label does not exist, then skip item
            entity_obj = SimpleNamespace()
            entity_obj.id = item_id
            entity_obj.label = item_label
            entity_obj.description = item_description
            entity_obj.aliases = item_aliases
            entity_obj.claims = json.loads(item['claims'])

            chunks = textifier.chunk_text(
                entity_obj,
                graph_store.tokenizer,
                max_length=graph_store.max_token_size
            )

            for chunk_i, chunk in enumerate(chunks):
                md5_hash = hashlib.md5(chunk.encode('utf-8')).hexdigest()
                metadata = {
                    "MD5": md5_hash,
                    "Label": item_label,
                    "Description": item_description,
                    "Aliases": item_aliases,
                    "Date": datetime.now().isoformat(),
                    "QID": item_id,
                    "ChunkID": chunk_i + 1,
                    "Language": LANGUAGE,
                    "IsItem": ('Q' in item_id),
                    "IsProperty": ('P' in item_id),
                    "DumpDate": DUMPDATE
                }

                graph_store.add_document(
                    id=f"{item_id}_{LANGUAGE}_{chunk_i+1}",
                    text=chunk,
                    metadata=metadata
                )

        progress_bar.value += 1

    while True:
        # Leftover Maintenance: Ensure that the batch is emptied out
        if not graph_store.push_batch():  # Stop when batch is empty
            break

if __name__ == "__main__":
    queue = Queue(maxsize=QUEUE_SIZE)
    progress_bar = Manager().Value("i", 0)

    with tqdm(total=total_entities) as pbar:
        processes = []
        for _ in range(NUM_PROCESSES):
            p = Process(target=process_items, args=(queue, progress_bar))
            p.start()
            processes.append(p)

        for item in dataset:
            queue.put(item)
            pbar.update(progress_bar.value - pbar.n)
            # pbar.n = progress_bar.value
            # pbar.refresh()

        for _ in range(NUM_PROCESSES):
            queue.put(None)

        while any(p.is_alive() for p in processes):
            pbar.update(progress_bar.value - pbar.n)
            # pbar.n = progress_bar.value
            # pbar.refresh()
            time.sleep(1)

        for p in processes:
            p.join()