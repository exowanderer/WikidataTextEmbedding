# docker compose run --build -e QUERY_LANGUAGE="ar" -e DB_LANGUAGE="ar" run_retrieval
docker compose run --build -e QUERY_LANGUAGE="en" -e DB_LANGUAGE="ar" run_retrieval
# docker compose run --build -e QUERY_LANGUAGE="de" -e DB_LANGUAGE="de" run_retrieval
docker compose run --build -e QUERY_LANGUAGE="en" -e DB_LANGUAGE="de" run_retrieval
docker compose run --build -e QUERY_LANGUAGE="en" -e DB_LANGUAGE="en" run_retrieval
# docker compose run --build -e QUERY_LANGUAGE="ar" -e DB_LANGUAGE="en" run_retrieval
# docker compose run --build -e QUERY_LANGUAGE="de" -e DB_LANGUAGE="en" run_retrieval

# docker compose run --build -e QUERY_LANGUAGE="ar" -e DB_LANGUAGE="en,ar" run_retrieval
docker compose run --build -e QUERY_LANGUAGE="en" -e DB_LANGUAGE="en,ar" run_retrieval
# docker compose run --build -e QUERY_LANGUAGE="de" -e DB_LANGUAGE="en,de" run_retrieval
docker compose run --build -e QUERY_LANGUAGE="en" -e DB_LANGUAGE="en,de" run_retrieval

# docker compose run --build -e QUERY_LANGUAGE="ar" -e DB_LANGUAGE="" run_retrieval
docker compose run --build -e QUERY_LANGUAGE="en" -e DB_LANGUAGE="" run_retrieval
# docker compose run --build -e QUERY_LANGUAGE="de" -e DB_LANGUAGE="" run_retrieval