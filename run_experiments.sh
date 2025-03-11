# docker compose run --build add_wikidata_to_astra
# docker compose run --build -e EVALUATION_PATH="Mintaka/processed_dataframe.pkl" -e QUERY_COL="Question" -e PREFIX="_nonewlines" -e COLLECTION_NAME="wikidatav1" -e QUERY_LANGUAGE="en" -e DB_LANGUAGE="en" -e API_KEY="datastax_wikidata2.json" run_retrieval
# docker compose run --build -e EVALUATION_PATH="LC_QuAD/processed_dataframe.pkl" -e QUERY_COL="Question" -e PREFIX="_nonewlines" -e COLLECTION_NAME="wikidatav1" -e QUERY_LANGUAGE="en" -e DB_LANGUAGE="en" -e API_KEY="datastax_wikidata2.json" run_retrieval
# docker compose run --build -e EVALUATION_PATH="REDFM/processed_dataframe.pkl" -e QUERY_COL="Sentence" -e PREFIX="_nonewlines" -e COLLECTION_NAME="wikidatav1" -e QUERY_LANGUAGE="en" -e DB_LANGUAGE="en" -e API_KEY="datastax_wikidata2.json" run_retrieval
# docker compose run --build -e EVALUATION_PATH="REDFM/processed_dataframe.pkl" -e QUERY_COL="Sentence no entity" -e PREFIX="_nonewlines_noentity" -e COLLECTION_NAME="wikidatav1" -e QUERY_LANGUAGE="en" -e DB_LANGUAGE="en" -e API_KEY="datastax_wikidata2.json" run_retrieval
# docker compose run --build -e EVALUATION_PATH="RuBQ/processed_dataframe.pkl" -e QUERY_COL="Question" -e PREFIX="_nonewlines" -e COLLECTION_NAME="wikidatav1" -e QUERY_LANGUAGE="en" -e DB_LANGUAGE="en" -e API_KEY="datastax_wikidata2.json" run_retrieval
# docker compose run --build -e EVALUATION_PATH="Wikidata-Disamb/processed_dataframe.pkl" -e QUERY_COL="Sentence" -e COMPARATIVE="true" -e COMPARATIVE_COLS="Correct QID,Wrong QID" -e COLLECTION_NAME="wikidatav1" -e PREFIX="_nonewlines" -e QUERY_LANGUAGE="en" -e DB_LANGUAGE="en" -e API_KEY="datastax_wikidata2.json" run_retrieval

# docker compose run --build -e CHUNK_NUM=5 create_prototype
docker compose run --build -e CHUNK_NUM=6 create_prototype
docker compose run --build -e CHUNK_NUM=7 create_prototype
docker compose run --build -e CHUNK_NUM=8 create_prototype