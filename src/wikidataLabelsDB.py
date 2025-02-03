from sqlalchemy import Column, Text, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator
import json
import re

"""
SQLite database setup for storing Wikidata labels in all languages.
"""
engine = create_engine(f'sqlite:///../data/Wikidata/sqlite_wikidata_labels.db',
    pool_size=5,       # Limit the number of open connections
    max_overflow=10,   # Allow extra connections beyond pool_size
    pool_recycle=10    # Recycle connections every 10 seconds
)
Base = declarative_base()
Session = sessionmaker(bind=engine)

class JSONType(TypeDecorator):
    """Custom SQLAlchemy type for JSON storage in SQLite."""
    impl = Text
    cache_ok = False

    def process_bind_param(self, value, dialect):
        if value is not None:
            return json.dumps(value, separators=(',', ':'))
        return None

    def process_result_value(self, value, dialect):
        if value is not None:
            return json.loads(value)
        return None

class WikidataLabels(Base):
    """ Represents a Wikidata entity's labels in multiple languages."""

    __tablename__ = 'labels'

    id = Column(Text, primary_key=True)
    labels = Column(JSONType)

    @staticmethod
    def add_bulk_labels(data):
        """
        Insert multiple label records in bulk. If a record with the same ID exists,
        it is ignored (no update is performed).

        Parameters:
        - data (list[dict]): A list of dictionaries, each containing 'id' and 'labels' keys.

        Returns:
        - bool: True if the operation was successful, False otherwise.
        """
        worked = False
        with Session() as session:
            try:
                session.execute(
                    text(
                        """
                        INSERT INTO labels (id, labels)
                        VALUES (:id, :labels)
                        ON CONFLICT(id) DO NOTHING
                        """
                    ),
                    data
                )
                session.commit()
                session.flush()
                worked = True
            except Exception as e:
                session.rollback()
                print(e)
        return worked

    @staticmethod
    def add_labels(id, labels):
        """
        Insert a single label record into the database.

        Parameters:
        - id (str): The unique identifier for the entity.
        - labels (dict): A dictionary of labels (e.g. { "en": "Label in English", "fr": "Label in French", ... }).

        Returns:
        - bool: True if the operation was successful, False otherwise.
        """
        worked = False
        with Session() as session:
            try:
                new_entry = WikidataLabels(
                    id=id,
                    labels=labels
                )
                session.add(new_entry)
                session.commit()
                session.flush()
                worked = True
            except Exception as e:
                session.rollback()
                print(f"Error: {e}")
        return worked

    @staticmethod
    def get_labels(id):
        """
        Retrieve labels for a given entity by its ID.

        Parameters:
        - id (str): The unique identifier of the entity.

        Returns:
        - dict: The labels dictionary if found, otherwise an empty dict.
        """
        with Session() as session:
            labels = session.query(WikidataLabels).filter_by(id=id).first()
            if labels is not None:
                return labels.labels
            return {}

    @staticmethod
    def get_labels_list(id_list):
        """
        Retrieve labels for multiple entities at once.

        Parameters:
        - id_list (list[str]): A list of entity IDs.

        Returns:
        - dict: A mapping of {entity_id: labels_dict} for each found ID. Missing IDs won't appear.
        """
        with Session() as session:
            rows = (
                session.query(WikidataLabels.id, WikidataLabels.labels)
                    .filter(WikidataLabels.id.in_(id_list))
                    .all()
            )

        return {row_id: row_labels for row_id, row_labels in rows if row_labels is not None}

    @staticmethod
    def _remove_keys(data, keys_to_remove=['hash', 'property', 'numeric-id', 'qualifiers-order']):
        """
        Recursively remove specific keys from a nested data structure.

        Parameters:
        - data (dict or list): The data structure to clean.
        - keys_to_remove (list): Keys to remove. Default includes 'hash', 'property', 'numeric-id', and 'qualifiers-order'.

        Returns:
        - dict or list: The cleaned data structure with specified keys removed.
        """
        if isinstance(data, dict):
            data = {key: WikidataLabels._remove_keys(value, keys_to_remove) for key, value in data.items() if key not in keys_to_remove}
        elif isinstance(data, list):
            data = [WikidataLabels._remove_keys(item, keys_to_remove) for item in data]
        return data

    @staticmethod
    def _clean_datavalue(data):
        """
        Remove unnecessary nested structures unless they match a Wikidata entity or property pattern.

        Parameters:
        - data (dict or list): The data structure to clean.

        Returns:
        - dict or list: The cleaned data.
        """
        if isinstance(data, dict):
            # If there's only one key and it's not a property or QID, recurse into it.
            if (len(data.keys()) == 1) and not re.match(r"^[PQ]\d+$", list(data.keys())[0]):
                data = WikidataLabels._clean_datavalue(data[list(data.keys())[0]])
            else:
                data = {key: WikidataLabels._clean_datavalue(value) for key, value in data.items()}
        elif isinstance(data, list):
            data = [WikidataLabels._clean_datavalue(item) for item in data]
        return data

    @staticmethod
    def _gather_labels_ids(data):
        """
        Find and return all relevant Wikidata IDs (e.g., property, unit, or datavalue IDs) in the claims.

        Parameters:
        - data (dict or list): The data structure to scan.

        Returns:
        - list[str]: A list of discovered Wikidata IDs.
        """
        ids = set()

        if isinstance(data, dict):
            if 'property' in data:
                ids.add(data['property'])

            if 'unit' in data and data['unit'] != '1':
                unit_id = data['unit'].split('/')[-1]
                ids.add(unit_id)

            if ('datatype' in data
                and 'datavalue' in data
                and data['datatype'] in ('wikibase-item', 'wikibase-property')):
                ids.add(data['datavalue'])

            for value in data.values():
                sub_ids = WikidataLabels._gather_labels_ids(value)
                ids.update(sub_ids)

        elif isinstance(data, list):
            for item in data:
                sub_ids = WikidataLabels._gather_labels_ids(item)
                ids.update(sub_ids)

        return list(ids)

    @staticmethod
    def _add_labels_to_claims(data, labels_dict={}):
        """
        For each found ID (property, unit, or datavalue) within the claims,
        insert the corresponding labels from labels_dict or the database.

        Parameters:
        - data (dict or list): The claims data structure.
        - labels_dict (dict): An optional dict of {id: labels} for quick lookup.

        Returns:
        - dict or list: The updated data with added label information.
        """
        if isinstance(data, dict):
            if 'property' in data:
                if data['property'] in labels_dict:
                    labels = labels_dict[data['property']]
                else:
                    labels = WikidataLabels.get_labels(data['property'])

                data = {
                    **data,
                    'property-labels': labels
                }

            if ('unit' in data) and (data['unit'] != '1'):
                id = data['unit'].split('/')[-1]
                if id in labels_dict:
                    labels = labels_dict[id]
                else:
                    labels = WikidataLabels.get_labels(id)

                data = {
                    **data,
                    'unit-labels': labels
                }

            if ('datatype' in data) and ('datavalue' in data) and ((data['datatype'] == 'wikibase-item') or (data['datatype'] == 'wikibase-property')):
                if data['datavalue'] in labels_dict:
                    labels = labels_dict[data['datavalue']]
                else:
                    labels = WikidataLabels.get_labels(data['datavalue'])

                data['datavalue'] = {
                    'id': data['datavalue'],
                    'labels': labels
                }

            data = {key: WikidataLabels._add_labels_to_claims(value, labels_dict=labels_dict) for key, value in data.items()}

        elif isinstance(data, list):
            data = [WikidataLabels._add_labels_to_claims(item, labels_dict=labels_dict) for item in data]

        return data

    @staticmethod
    def add_labels_batched(claims, query_batch=100):
        """
        Gather all relevant IDs from claims, batch-fetch their labels, then add them to the claims structure.

        Parameters:
        - claims (dict or list): The claims data structure to update.
        - query_batch (int): The batch size for querying labels in groups. Default is 100.

        Returns:
        - dict or list: The updated claims with labels inserted.
        """
        label_ids = WikidataLabels._gather_labels_ids(claims)

        labels_dict = {}
        for i in range(0, len(label_ids), query_batch):
            temp_dict = WikidataLabels.get_labels_list(label_ids[i:i+query_batch])
            labels_dict = {**labels_dict, **temp_dict}

        claims = WikidataLabels._add_labels_to_claims(claims, labels_dict=labels_dict)
        return claims

    @staticmethod
    def clean_entity(entity):
        """
        Clean a Wikidata entity's data by removing unneeded keys and adding label info to claims.

        Parameters:
        - entity (dict): A Wikidata entity dictionary containing 'claims', 'labels', 'sitelinks', etc.

        Returns:
        - dict: The cleaned entity with label data integrated into its claims.
        """
        clean_claims = WikidataLabels._remove_keys(entity.get('claims', {}), ['hash', 'snaktype', 'type', 'entity-type', 'numeric-id', 'qualifiers-order', 'snaks-order'])
        clean_claims = WikidataLabels._clean_datavalue(clean_claims)
        clean_claims = WikidataLabels._remove_keys(clean_claims, ['id'])
        clean_claims = WikidataLabels.add_labels_batched(clean_claims)

        sitelinks = WikidataLabels._remove_keys(entity.get('sitelinks', {}), ['badges'])

        return {
            'id': entity['id'],
            'labels': entity['labels'],
            'descriptions': entity['descriptions'],
            'aliases': entity['aliases'],
            'sitelinks': sitelinks,
            'claims': clean_claims
        }

    @staticmethod
    def clean_labels(labels):
        labels = WikidataLabels._remove_keys(labels, ['language'])
        labels = WikidataLabels._clean_datavalue(labels)
        return labels

    @staticmethod
    def is_in_wikipedia(entity):
        """
        Check if a Wikidata entity has a corresponding Wikipedia entry in any language.

        Parameters:
        - entity (dict): A Wikidata entity dictionary.

        Returns:
        - bool: True if the entity has at least one sitelink ending in 'wiki', otherwise False.
        """
        if ('sitelinks' in entity):
            for s in entity['sitelinks']:
                if s.endswith('wiki'):
                    return True
        return False

# Create tables if they don't already exist.
Base.metadata.create_all(engine)