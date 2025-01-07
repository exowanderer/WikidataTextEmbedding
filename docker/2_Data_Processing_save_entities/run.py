import sys
sys.path.append('../src')

from wikidata_dumpreader import WikidataDumpReader
from wikidataDB import WikidataID, WikidataEntity
from multiprocessing import Manager
import asyncio
import os
import gc

FILEPATH = os.getenv("FILEPATH", '../data/Wikidata/latest-all.json.bz2')
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 1000))
QUEUE_SIZE = int(os.getenv("QUEUE_SIZE", 1500))
NUM_PROCESSES = int(os.getenv("NUM_PROCESSES", 4))
SKIPLINES = int(os.getenv("SKIPLINES", 0))
LANGUAGE = os.getenv("LANGUAGE", 'en')

def save_entities_to_sqlite(item, data_batch, sqlitDBlock):
    if (item is not None) and WikidataID.get_id(item['id']):
        item = WikidataEntity.normalise_item(item, language=LANGUAGE)
        data_batch.append(item)

        with sqlitDBlock:
            if len(data_batch) > BATCH_SIZE:
                worked = WikidataEntity.add_bulk_entities(list(data_batch[:BATCH_SIZE]))
                if worked:
                    del data_batch[:BATCH_SIZE]
                    gc.collect()

async def run_processor(wikidata, bulk_ids, sqlitDBlock):
    await wikidata.run(lambda item: save_entities_to_sqlite(item, bulk_ids, sqlitDBlock), max_iterations=None, verbose=True)

if __name__ == "__main__":
    multiprocess_manager = Manager()
    sqlitDBlock = multiprocess_manager.Lock()
    data_batch = multiprocess_manager.list()

    wikidata = WikidataDumpReader(FILEPATH, num_processes=NUM_PROCESSES, batch_size=BATCH_SIZE, queue_size=QUEUE_SIZE, skiplines=SKIPLINES)

    asyncio.run(run_processor(wikidata, data_batch, sqlitDBlock))

    while len(data_batch) > 0:
        worked = WikidataEntity.add_bulk_entities(list(data_batch))
        if worked:
            data_batch[:] = []
        else:
            asyncio.sleep(1)