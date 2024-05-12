
if __name__ == '__main__':
    if 'embedder' not in locals():
        embedder = SentenceTransformer(
            "jinaai/jina-embeddings-v2-base-en",
            trust_remote_code=True
        )

    lang = 'en'
    timeout = 10
    n_cores = cpu_count()
    verbose = False
    wikidata_base = '"wikidata.org"'
    return_list = True
    version = 1
    n_qids = 10000  # number of QIDs to embed

    qids = [f'Q{k+1}' for k in range(n_qids)]

    save_filename = f'wikidata_vectordb_sparql_{n_qids}_{lang}.csv'

    wd_textification = WikidataTextification(
        embedder=embedder,
        lang=lang,
        timeout=timeout,
        n_cores=n_cores,
        verbose=verbose,
        wikidata_base=wikidata_base,
        return_list=return_list,
        save_filename=save_filename
    )

    start = time()
    has_all_qids = False  # Start as do_while
    while not has_all_qids:
        try:
            wd_textification.create_vecdb(qids)
        except Exception as e:
            print(f'Error: {e}')

        has_vecdb = hasattr(wd_textification, 'df_vecdb')
        if has_vecdb:
            len_unique_qids = len(wd_textification.df_vecdb.qid.unique())
            has_all_qids = len(qids) != len_unique_qids

    print(f'Operation took {time() - start}')
    print(wd_textification.df_vecdb)
