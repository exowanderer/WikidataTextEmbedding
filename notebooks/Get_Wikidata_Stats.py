# # How to read and process the Wikdata dump file.

import sys
sys.path.append('../src')

from wikidataDumpReader import WikidataDumpReader
from multiprocessing import Manager

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
    label_langs = set(entity.get('labels', {}).keys())
    return label_langs

def get_wikidata_descr_lang(entity):
    """
    Return the languages supported in this Wikidata entity (label and description)
    """
    desc_langs = set(entity.get('descriptions', {}).keys())
    return desc_langs

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

def get_instanceof(entity):
    """
    Return the instance of QID values
    """
    instanceof = set()
    if 'P31' in entity.get('claims', {}):
        for c in entity['claims']['P31']:
            if ('mainsnak' in c) and ('datavalue' in c['mainsnak']):
                if (c['mainsnak'].get('datatype', '') == 'wikibase-item'):
                    qid = c['mainsnak']['datavalue']['value']['id']
                    instanceof.add(qid)
    return instanceof


def calculate_stats(
    item, item_type_count, wikidatalabellang_counts, wikidatadescrlang_counts,
    wikipedialang_counts, wikidatalang_counts, wikidatalang_counts_wikionly,
    claim_counts, claim_counts_wikionly, instanceof_counts,
    instanceof_counts_wikionly, wikidata_wikipedia_lang_counts):
    """
    Calculate various statistics for a given Wikidata item.
    """

    if item is not None:
        id_type = item['id'][0]
        wikipedialangs = get_wikipedia_lang(item)

        label_langs = get_wikidata_label_lang(item)
        desc_langs = get_wikidata_descr_lang(item)
        wikidatalangs = label_langs.intersection(desc_langs)

        claimspids = get_claims_pids(item)
        instanceof = get_instanceof(item)

        item_type_count[id_type] = item_type_count.get(id_type, 0) +1


        if id_type == 'Q':
            update_counts(
                wikipedialangs,
                label_langs,
                desc_langs,
                wikidatalangs,
                claimspids,
                instanceof, 
                wikipedialang_counts,
                wikidatalabellang_counts,
                wikidatadescrlang_counts, 
                wikidatalang_counts,
                claim_counts,
                instanceof_counts, 
                wikidatalang_counts_wikionly,
                claim_counts_wikionly, 
                instanceof_counts_wikionly,wikidata_wikipedia_lang_counts
            )

def update_counts(
    wikipedialangs: set, label_langs: set, desc_langs: set, wikidatalangs: set, 
    claimspids: dict, instanceof: set, wikipedialang_counts: dict, 
    wikidatalabellang_counts: dict, wikidatadescrlang_counts: dict, 
    wikidatalang_counts: dict, claim_counts: dict, instanceof_counts: dict, 
    wikidatalang_counts_wikionly: dict, claim_counts_wikionly: dict, 
    instanceof_counts_wikionly: dict, wikidata_wikipedia_lang_counts: dict
    ) -> None:

        for lang in wikipedialangs:
            wikipedialang_counts[lang] = wikipedialang_counts.get(lang, 0) +1

        for lang in label_langs:
            wikidatalabellang_counts[lang] = wikidatalabellang_counts.get(lang, 0) +1
        for lang in desc_langs:
            wikidatadescrlang_counts[lang] = wikidatadescrlang_counts.get(lang, 0) +1

        for lang in wikidatalangs:
            wikidatalang_counts[lang] = wikidatalang_counts.get(lang, 0) +1

        for pid, count in claimspids.items():
            claim_counts[pid] = claim_counts.get(pid, 0) + count

        for qid in instanceof:
            instanceof_counts[qid] = instanceof_counts.get(qid, 0) +1

        if len(wikipedialangs) > 0:
            wikipedialang_counts['total']  = wikipedialang_counts.get('total', 0) +1

            for lang in wikidatalangs:
                wikidatalang_counts_wikionly[lang] = wikidatalang_counts_wikionly.get(lang, 0) +1

            for pid, count in claimspids.items():
                claim_counts_wikionly[pid] = claim_counts_wikionly.get(pid, 0) + count

            for qid in instanceof:
                instanceof_counts_wikionly[qid] = instanceof_counts_wikionly.get(qid, 0) +1

        for lang in wikidatalangs.intersection(wikipedialangs):
            wikidata_wikipedia_lang_counts[lang] = wikidata_wikipedia_lang_counts.get(lang, 0) +1


if __name__ == '__main__':
    import os

    from wikidataDumpReader import WikidataDumpReader
    from multiprocessing import Manager
    # FILEPATH = '../data/Wikidata/latest-all.json.bz2'
    FILEDIR = '/mnt/wwn-0x5000c500f7e371c0-part1/WikidataUnplugged/'
    FILENAME = 'latest-all-Sept26.json' # .bz2'
    FILEPATH = os.path.join(FILEDIR, FILENAME)

    QUEUE_SIZE = 15000
    NUM_PROCESSES = 4
    SKIPLINES = 0

    # Initialize multiprocessing manager
    multiprocess_manager = Manager()

    # Section: Shared dictionaries for statistics

    # Per language, count the the items that are connected to the Wikipedia 
    # page of the language
    wikipedialang_counts = multiprocess_manager.dict() 

    # Per language, count the the items that have a label supported in the language
    wikidatalabellang_counts = multiprocess_manager.dict() 

    # Per language, count the the items that have a description 
    # supported in the language
    wikidatadescrlang_counts = multiprocess_manager.dict() 

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
    instanceof_counts = multiprocess_manager.dict() 

    # Same as instanceof_counts but for items that are connected to a Wikipedia page
    instanceof_counts_wikionly = multiprocess_manager.dict()

    # Number of QIDs vs PIDs vs LIDs...
    item_type_count = multiprocess_manager.dict()

    wikidata = WikidataDumpReader(
        FILEPATH,
        num_processes=NUM_PROCESSES,
        queue_size=QUEUE_SIZE,
        skiplines=SKIPLINES
    )

    wikidata.run(
        lambda item: calculate_stats(
            item, item_type_count,
            wikidatalabellang_counts,
            wikidatadescrlang_counts,
            wikipedialang_counts,
            wikidatalang_counts,
            wikidatalang_counts_wikionly,
            claim_counts,
            claim_counts_wikionly,
            instanceof_counts,
            instanceof_counts_wikionly,
            wikidata_wikipedia_lang_counts
        ),
        # max_iterations=10000,
        verbose=True
    )
