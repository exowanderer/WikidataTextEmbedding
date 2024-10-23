# Docker Containers for Processing Wikidata Data

This project contains several Docker containers to process the Wikidata Dump and save relevant data in a SQLite database, and then push the data to a DataStax vector database. Please run the containers sequentially as described below.

## Step 1: Data Processing - Save IDs
This container processes the Wikidata Dump ZIP file and saves only the QIDs and PIDs of Wikidata Entities that are connected to English Wikipedia, along with one level of connected Entities and Properties. The IDs are saved in a SQLite database.

To run the container:

```bash
docker compose up data_processing_save_ids --build
```

## Step 2: Data Processing - Save Entities
After saving the relevant QIDs and PIDs, this container processes the Wikidata dump file again and saves the relevant data of entities and properties into the SQLite database.

To run the container:

```bash
docker compose up data_processing_save_entities --build
```

## Step 3: Add Wikidata to Astra
Once all entities are saved, this container processes the saved entities in SQLite (those connected to English Wikipedia) and pushes them to the DataStax vector database, using the provided API keys.

To run the container:

```bash
docker compose up add_wikidata_to_astra --build
```