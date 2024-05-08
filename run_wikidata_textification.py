n_qids = 10  # number of QIDs to embed

# List of the first `n_qids` QIDs
qids = [f'Q{k+1}' for k in range(n_qids)]

if 'embedder' not in locals():
    embedder = SentenceTransformer(
        "jinaai/jina-embeddings-v2-base-en",
        trust_remote_code=True
    )

logger = WikidataTextification.get_logger(__name__)

wd_textification = WikidataTextification(
    embedder=embedder,
    lang='en',
    timeout=10,
    n_cores=cpu_count(),
    verbose=False,
    wikidata_base='"wikidata.org"',
    return_list=True,
    # serapi_api_key=None
)

start = time()
wd_statements = wd_textification.get_wikidata_statements(qids)
logger.debug(time() - start)
logger.debug(len(wd_statements))

df_vecdb = pd.DataFrame(wd_statements)

df_vecdb.to_csv(f'wikidata_vectordb_first_{n_qids}.csv')
