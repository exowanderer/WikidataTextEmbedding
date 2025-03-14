import sys
sys.path.append('../src')

from wikidataDumpReader import WikidataDumpReader
from wikidataDB import WikidataID
from multiprocessing import Manager
import os
import time

FILEPATH = os.getenv("FILEPATH", '../data/Wikidata/latest-all.json.bz2')
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
QUEUE_SIZE = int(os.getenv("QUEUE_SIZE", 1500))
NUM_PROCESSES = int(os.getenv("NUM_PROCESSES", 4))
SKIPLINES = int(os.getenv("SKIPLINES", 0))
LANGUAGE = os.getenv("LANGUAGE", 'en')

def save_ids_to_sqlite(item, bulk_ids, sqlitDBlock):
    if (item is not None) and WikidataID.is_in_wikipedia(item, language=LANGUAGE):
        ids = WikidataID.extract_entity_ids(item, language=LANGUAGE)
        bulk_ids.extend(ids)

        with sqlitDBlock:
            if len(bulk_ids) > BATCH_SIZE:
                worked = WikidataID.add_bulk_ids(list(bulk_ids[:BATCH_SIZE]))
                if worked:
                    del bulk_ids[:BATCH_SIZE]

if __name__ == "__main__":
    multiprocess_manager = Manager()
    sqlitDBlock = multiprocess_manager.Lock()
    bulk_ids = multiprocess_manager.list()

    wikidata = WikidataDumpReader(FILEPATH, num_processes=NUM_PROCESSES, batch_size=BATCH_SIZE, queue_size=QUEUE_SIZE, skiplines=SKIPLINES)
    wikidata.run(lambda item: save_ids_to_sqlite(item, bulk_ids, sqlitDBlock), max_iterations=None, verbose=True)

    while len(bulk_ids) > 0:
        worked = WikidataID.add_bulk_ids(list(bulk_ids))
        if worked:
            bulk_ids[:] = []
        else:
            time.sleep(1)