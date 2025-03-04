# # How to read and process the Wikdata dump file.

import sys
sys.path.append('../src')

from wikidataDumpReader import WikidataDumpReader
from multiprocessing import Manager, cpu_count

def get_wikipedia_lang(entity):
    """
    Return the languages of all Wikipedia pages connected to the Wikidata entity.
    """
    langs = set()
    if ('sitelinks' in entity):
        for s in entity['sitelinks']:
            if s.endswith('wiki'):
                langs.add(s.split('wiki')[0])
    return langs

def get_wikidata_label_lang(entity):
    """
    Return the languages supported in this Wikidata entity (label and description)
    """
    wikidata_label_langs = set(entity.get('labels', {}).keys())
    return wikidata_label_langs

def get_wikidata_desc_lang(entity):
    """
    Return the languages supported in this Wikidata entity (label and description)
    """
    wikidata_desc_langs = set(entity.get('descriptions', {}).keys())
    return wikidata_desc_langs

def get_claims_pids(entity):
    """
    Return the list of properties connected to the Wikidata entity.
    """
    pids_count = {}
    for pid,claim in entity.get('claims', {}).items():
        pids_count[pid] = pids_count.get(pid, 0) +1

        for c in claim:
            if 'qualifiers' in c:
                for pid,_ in c['qualifiers'].items():
                    pids_count[pid] = pids_count.get(pid, 0) +1
    return pids_count

def get_instance_of(entity):
    """
    Return the instance of QID values
    """
    instance_of = set()
    if 'P31' in entity.get('claims', {}):
        for c in entity['claims']['P31']:
            if ('mainsnak' in c) and ('datavalue' in c['mainsnak']):
                if (c['mainsnak'].get('datatype', '') == 'wikibase-item'):
                    qid = c['mainsnak']['datavalue']['value']['id']
                    instance_of.add(qid)
    return instance_of


def calculate_stats(item, counters):
    """
    Calculate various statistics for a given Wikidata item.
    """

    if item is not None:
        id_type = item['id'][0]
        wikipedia_langs = get_wikipedia_lang(item)

        wikidata_label_langs = get_wikidata_label_lang(item)
        wikidata_desc_langs = get_wikidata_desc_lang(item)
        wikidata_lbldesc_langs = wikidata_label_langs.intersection(wikidata_desc_langs)

        claims_pids = get_claims_pids(item)
        instance_of = get_instance_of(item)

        item_type_count = counters['item_type']  # temp variable
        counters['item_type'][id_type] = item_type_count.get(id_type, 0) +1

        if id_type == 'Q':
            update_counts(
                wikipedia_langs,
                wikidata_label_langs,
                wikidata_desc_langs,
                wikidata_lbldesc_langs,
                claims_pids,
                instance_of,
                counters
            )

def update_counts(
    wikipedia_langs: set, wikidata_label_langs: set, wikidata_desc_langs: set,
    wikidata_langs: set, claims_pids: dict, instance_of: set, 
    counters: dict) -> None:

    for lang in wikipedia_langs:
        counters['wikipedia_lang'][lang] = counters['wikipedia_lang'].get(lang, 0) +1

    for lang in wikidata_label_langs:
        counters['wikidata_label_lang'][lang] = counters['wikidata_label_lang'].get(lang, 0) +1

    for lang in wikidata_desc_langs:
        counters['wikidata_desc_lang'][lang] = counters['wikidata_desc_lang'].get(lang, 0) +1

    for lang in wikidata_langs:
        counters['wikidata_lang'][lang] = counters['wikidata_lang'].get(lang, 0) +1

    for pid, count in claims_pids.items():
        counters['claim_pid'][pid] = counters['claim_pid'].get(pid, 0) + count

    for qid in instance_of:
        counters['instance_of'][qid] = counters['instance_of'].get(qid, 0) +1

    if len(wikipedia_langs) > 0:
        counters['wikipedia_lang']['total']  = counters['wikipedia_lang'].get('total', 0) +1

        for lang in wikidata_langs:
            counters['wikidata_lang_wikionly'][lang] = counters['wikidata_lang_wikionly'].get(lang, 0) +1

        for pid, count in claims_pids.items():
            counters['claim_pid_wikionly'][pid] = counters['claim_pid_wikionly'].get(pid, 0) + count

        for qid in instance_of:
            counters['instance_of_wikionly'][qid] = counters['instance_of_wikionly'].get(qid, 0) +1

    for lang in wikidata_langs.intersection(wikipedia_langs):
        counters['wikidata_wikipedia_lang'][lang] = counters['wikidata_wikipedia_lang'].get(lang, 0) +1


if __name__ == '__main__':
    import os

    import sys
    sys.path.append('../src')

    from wikidataDumpReader import WikidataDumpReader
    from multiprocessing import Manager, cpu_count
    from Get_Wikidata_Stats import calculate_stats

    # FILEPATH = '../data/Wikidata/latest-all.json.bz2'
    FILEDIR = '/mnt/wwn-0x5000c500f7e371c0-part1/WikidataUnplugged/'
    FILENAME = 'latest-all-Sept26.json' # .bz2'
    FILEPATH = os.path.join(FILEDIR, FILENAME)

    QUEUE_SIZE = 15000
    NUM_PROCESSES = cpu_count() - 1
    SKIPLINES = 0

    # Initialize multiprocessing manager
    multiprocess_manager = Manager()

    # Shared dictionaries for statistics
    counter_names = [
        # Per language, count the the items that are connected to the Wikipedia page of the language
        'wikipedia_lang',

        # Per language, count the the items that have a label supported in the language
        'wikidata_label_lang',

        # Per language, count the the items that have a description supported in the language
        'wikidata_desc_lang',

        # Per language, count the the items that have a label and description supported in the language
        'wikidata_lang',

        # Same as wikidatalang but for items that are connected to a Wikipedia page
        'wikidata_lang_wikionly',

        # The intersection of wikipedialang and wikidatalang
        'wikidata_wikipedia_lang',

        # Per claim, count how many times it's been included in an item
        'claim_pid',

        # Same as claim but for items that are connected to a Wikipedia page
        'claim_pid_wikionly',

        # Count the distinct values of instance of claim
        'instance_of',

        # Same as instance_of but for items that are connected to a Wikipedia page
        'instance_of_wikionly',

        # Number of QIDs vs PIDs vs LIDs...
        'item_type'
    ]
    counters = {cname:multiprocess_manager.dict() for cname in counter_names}

    """
    wikipedialang_counts = multiprocess_manager.dict() 

    # Per language, count the the items that have a label supported in the language
    wikidatalabellang_counts = multiprocess_manager.dict() 

    # Per language, count the the items that have a description 
    # supported in the language
    wikidatadesclang_counts = multiprocess_manager.dict() 

    # Per language, count the the items that have a label and description 
    # supported in the language
    wikidatalang_counts = multiprocess_manager.dict() 

    # Same as wikidatalang_counts but for items that are connected 
    # to a Wikipedia page
    wikidatalang_counts_wikionly = multiprocess_manager.dict() 

    # The intersection of wikipedialang_counts and wikidatalang_counts.
    wikidata_wikipedia_lang_counts = multiprocess_manager.dict() 

    # Per claim, count how many times it's been included in an item
    claim_counts = multiprocess_manager.dict() 

    # Same as claim_counts but for items that are connected to a Wikipedia page
    claim_counts_wikionly = multiprocess_manager.dict() 

    # Count the distinct values of instance of claim
    instance_of_counts = multiprocess_manager.dict() 

    # Same as instance_of_counts but for items that are connected to a Wikipedia page
    instance_of_counts_wikionly = multiprocess_manager.dict()

    # Number of QIDs vs PIDs vs LIDs...
    item_type_count = multiprocess_manager.dict()
    """

    # Create a WikidataDumpReader object to process counters
    wikidata = WikidataDumpReader(
        FILEPATH,
        num_processes=NUM_PROCESSES,
        queue_size=QUEUE_SIZE,
        skiplines=SKIPLINES
    )

    wikidata.run(
        lambda item: calculate_stats(item, counters),
        # max_iterations=10000,
        verbose=True
    )
