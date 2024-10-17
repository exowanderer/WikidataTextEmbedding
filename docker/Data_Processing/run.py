import sys
sys.path.append('../src')

from wikidata_dumpreader import WikidataDumpReader
from wikidataDB import WikidataID, WikidataEntity, Session
from multiprocessing import Manager
from sqlalchemy import select
import asyncio
import gc

FILEPATH = '../data/Wikidata/latest-all.json.bz2'
BATCH_SIZE = 1000
QUEUE_SIZE = 1500
NUM_PROCESSES = 4
SKIPLINES = 0
LANGUAGE = 'en'

def save_ids_to_sqlite(item, bulk_ids, sqlitDBlock):
    if (item is not None) and WikidataID.is_in_wikipedia(item, language=LANGUAGE):
        ids = WikidataID.extract_entity_ids(item, language=LANGUAGE)
        bulk_ids.extend(ids)
        del item

        with sqlitDBlock:
            if len(bulk_ids) > BATCH_SIZE:
                worked = WikidataID.add_bulk_ids(list(bulk_ids[:BATCH_SIZE]))
                if worked:
                    del bulk_ids[:BATCH_SIZE]
                    gc.collect()

async def run_processor(wikidata, bulk_ids, sqlitDBlock):
    await wikidata.run(lambda item: save_ids_to_sqlite(item, bulk_ids, sqlitDBlock), max_iterations=None, verbose=True)

if __name__ == "__main__":
    multiprocess_manager = Manager()
    sqlitDBlock = multiprocess_manager.Lock()
    bulk_ids = multiprocess_manager.list()

    wikidata = WikidataDumpReader(FILEPATH, num_processes=NUM_PROCESSES, batch_size=BATCH_SIZE, queue_size=QUEUE_SIZE, skiplines=SKIPLINES)

    asyncio.run(run_processor(wikidata, bulk_ids, sqlitDBlock))

    while len(bulk_ids) > 0:
        worked = WikidataID.add_bulk_ids(list(bulk_ids))
        if worked:
            bulk_ids[:] = []
        else:
            asyncio.sleep(1)