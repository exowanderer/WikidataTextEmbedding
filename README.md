# Wikidata Embedding Project

## Overview
The Wikidata Embedding Project is an initiative led by Wikimedia Deutschland, in collaboration with [Jina.AI](https://jina.ai/) and [DataStax](https://www.datastax.com/). The project’s aim is to enhance the search functionality of Wikidata by integrating advanced vector-based semantic search. By employing advanced machine learning models and scalable vector databases, the project seeks to support the open-source community in developing innovative AI applications and use Wikidata's multilingual and inclusive knowledge graph, while making its extensive data more accessible, and contextually relevant for users across the globe.

For more details, visit [the Wikidata Embedding Project page](https://www.wikidata.org/wiki/Wikidata:Embedding_Project).

## Getting Started
This project contains several Docker containers to process the Wikidata Dump and save relevant data in a SQLite database, and then push the data to a DataStax vector database. Please run the containers sequentially as described below.

---

### Container: `data_processing_save_ids`

#### Functionality
This container reads the Wikidata dump and extracts only the entity IDs and property IDs, storing them in an SQLite database. It only considers entities linked to Wikipedia in a specified language. Additionally, it extracts and saves IDs found inside the claims of the filtered items, ensuring all necessary elements are available for constructing textual representations.

#### Environment Variables
| Variable        | Default Value | Description |
|-----------------|--------------|-------------|
| `FILEPATH`      | `../data/Wikidata/latest-all.json.bz2` | Path to the Wikidata dump file |
| `BATCH_SIZE`    | `1000`        | Number of gathered IDs before pushing to SQLite |
| `QUEUE_SIZE`    | `1500`        | Size of the queue buffering processed lines from the dump |
| `NUM_PROCESSES` | `4`           | Number of processes filtering entities & extracting IDs from claims |
| `SKIPLINES`     | `0`           | Number of lines to skip in the data dump (useful for resuming processing) |
| `LANGUAGE`      | `'en'`        | Language filter (only entities linked to Wikipedia in this language are included) |

---

### Container: `data_processing_save_entities`

#### Functionality
This container processes the Wikidata dump again, extracting the data only for entities whose IDs are already stored in SQLite. It stores their labels, descriptions, aliases, and claims in the specified language and saves them into the SQLite database.

#### Environment Variables
| Variable        | Default Value | Description |
|-----------------|--------------|-------------|
| `FILEPATH`      | `../data/Wikidata/latest-all.json.bz2` | Path to the Wikidata dump file |
| `BATCH_SIZE`    | `1000`        | Number of gathered entities before pushing to SQLite |
| `QUEUE_SIZE`    | `1500`        | Size of the queue buffering processed lines from the dump |
| `NUM_PROCESSES` | `4`           | Number of processes filtering entities & preprocessing the data |
| `SKIPLINES`     | `0`           | Number of lines to skip in the data dump (useful for resuming processing) |
| `LANGUAGE`      | `'en'`        | Language filter (only the labels, descriptions, and aliases in this language are stored to SQLite) |

---

### Container: `add_wikidata_to_astra`

#### Functionality
This container processes the entities stored in the SQLite database, gathers all relevant information, and constructs a textual representation of each entity. The generated text is then embedded and stored in DataStax’s Astra DB vector database for efficient semantic search.

Before running this container, ensure that a formatting script exists for the specified language. These scripts are located in src/language_variables and define how entity attributes (labels, descriptions, aliases, and claims) should be formatted for each language. These scripts ensure that every language is processed correctly with appropriate translations.

#### Environment Variables
| Variable              | Default Value | Description |
|----------------------|--------------|-------------|
| `MODEL`             | `jina`        | Embedding model used [`jina-embeddings-v3`](https://huggingface.co/jinaai/jina-embeddings-v3) |
| `SAMPLE`           | `false`        | If `true`, only sample data is pushed for testing purposes |
| `EMBED_BATCH_SIZE`  | `100`         | Number of entities uploaded per batch |
| `QUERY_BATCH_SIZE`  | `1000`        | Number of entities extracted from SQLite per batch |
| `OFFSET`            | `0`           | SQLite offset (useful for resuming processing) |
| `API_KEY_FILENAME`  | `None`        | Path to the DataStax API key |
| `COLLECTION_NAME`   | `None`        | Name of the DataStax collection |
| `LANGUAGE`          | `'en'`        | Language of the SQLite database |
| `TEXTIFIER_LANGUAGE`| `LANGUAGE`    | Name of the Python script in `src/language_variables` |
| `DUMPDATE`          | `09/18/2024`  | Date of the Wikidata data dump |

---

### Container: `run_retrieval`

#### Functionality
This container queries the constructed Vector Database using an evaluation dataset and stores the retrieved QIDs and embedding scores per query.

#### Environment Variables
| Variable            | Default Value | Description |
|--------------------|--------------|-------------|
| `MODEL`           | `jina`        | Embedding model used [`jina-embeddings-v3`](https://huggingface.co/jinaai/jina-embeddings-v3) |
| `BATCH_SIZE`      | `100`         | Number of queries processed per batch |
| `API_KEY_FILENAME`| `None`        | Path to the DataStax API key |
| `COLLECTION_NAME` | `None`        | Name of the DataStax collection |
| `EVALUATION_PATH` | `None`        | Path to the evaluation dataset (pickled pandas dataframe) |
| `K`               | `50`          | Number of QIDs to retrieve per query |
| `QUERY_COL`       | `None`        | Column in the evaluation dataset containing the query text |
| `QUERY_LANGUAGE`  | `'en'`        | Language of the query text |
| `DB_LANGUAGE`     | `None`        | Language of the database entities. If specified, only entities in the given languages (comma-separated) are retrieved from the vector database (e.g., 'en,de')|
| `RESTART`         | `false`       | If `true`, restarts the retrieval process and deletes previous results |
| `COMPARATIVE`     | `false`       | If `true`, only specified entity QIDs are retrieved for distance comparison |
| `COMPARATIVE_COLS`| `None`        | Columns in the pandas dataframe for comparative evaluation where the QIDs are specified (comma separated) |
| `PREFIX`          | `''`          | Prefix for stored retrieval results |

---
