from sqlalchemy import Column, Text, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator
import json
import re

engine = create_engine(f'sqlite:///../data/Wikidata/sqlite_wikidata_labels.db',
    pool_size=5,       # Limit the number of open connections
    max_overflow=10,   # Allow extra connections beyond pool_size
    pool_recycle=10  # Recycle connections every 10 seconds
)
Base = declarative_base()
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

class JSONType(TypeDecorator):
    """Custom SQLAlchemy type for JSON storage in SQLite."""
    impl = Text
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            return json.dumps(value, separators=(',', ':'))
        return None

    def process_result_value(self, value, dialect):
        if value is not None:
            return json.loads(value)
        return None

class WikidataLabels(Base):
    __tablename__ = 'labels'

    id = Column(Text, primary_key=True)
    labels = Column(JSONType)

    @staticmethod
    def add_bulk_labels(data):
        """
        Add multiple entities to the database in bulk, if the item already exists then it's ignored.

        Parameters:
        - data: A list of dictionaries representing entities to be added.

        Returns:
        - True if the operation was successful, False otherwise.
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
        Add a single entity to the database.

        Parameters:
        - id: The unique identifier for the entity.
        - label: The label of the entity.
        - description: The description of the entity.
        - claims: The claims related to the entity in JSON format.
        - aliases: The aliases for the entity in JSON format.

        Returns:
        - True if the operation was successful, False otherwise.
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
        Retrieve an entity by ID

        Parameters:
        - id: The unique identifier of the entity to be retrieved.

        Returns:
        - The entity labels if found, otherwise None.
        """
        with Session() as session:
            labels = session.query(WikidataLabels).filter_by(id=id).first()
            if labels is not None:
                return labels.labels
            return {}

    @staticmethod
    def get_labels_list(id_list):
        """
        Given a list of entity IDs, return a dict mapping {id: labels_dict}.
        If an ID is missing from the table, it won't appear in the returned dict.

        Parameters:
        - id_list: A list of unique identifier of the entities to be retrieved.

        Returns:
        - A dictionary of the entity labels.
        """
        with Session() as session:
            rows = (
                session.query(WikidataLabels.id, WikidataLabels.labels)
                    .filter(WikidataLabels.id.in_(id_list))
                    .all()
            )

        return {row_id: row_labels for row_id, row_labels in rows if row_labels is not None}

    @staticmethod
    def _remove_keys(data, keys_to_remove):
        """Removes all keys in a nested dictionary that are in the keys_to_remove list.

        Args:
            data (dict): Dictionary to process
            keys_to_remove (list): A list of strings representing the keys to remove.

        Returns:
            dict: A cleaned-up dictionary.
        """
        if isinstance(data, dict):
            data = {key: WikidataLabels._remove_keys(value, keys_to_remove) for key, value in data.items() if key not in keys_to_remove}
        elif isinstance(data, list):
            data = [WikidataLabels._remove_keys(item, keys_to_remove) for item in data]
        return data

    @staticmethod
    def _clean_datavalue(data):
        """Remove unnecessary nested arrays or dictionaries with one key. Keep keys that represent a Wikidata property or entity ID.

        Args:
            data (dict): Dictionary to process

        Returns:
            dict: A cleaned-up dictionary.
        """
        if isinstance(data, dict):
            if (len(data.keys()) == 1) and not re.match(r"^[PQ]\d+$", list(data.keys())[0]):
                data = WikidataLabels._clean_datavalue(data[list(data.keys())[0]])
            else:
                data = {key: WikidataLabels._clean_datavalue(value) for key, value in data.items()}
        elif isinstance(data, list):
            data = [WikidataLabels._clean_datavalue(item) for item in data]
        return data

    @staticmethod
    def _gather_labels_ids(data):
        """Find and return all relevant Wikidata IDs (property, unit, or datavalue) within the claims.

        Args:
            data (dict): Dictionary to process

        Returns:
            dict: The dictionary with the added labels
        """
        ids = set()

        if isinstance(data, dict):
            # If there's a 'property' key, record its value
            if 'property' in data:
                ids.add(data['property'])

            # If there's a 'unit' that's not "1", record its final path component
            if 'unit' in data and data['unit'] != '1':
                unit_id = data['unit'].split('/')[-1]
                ids.add(unit_id)

            # If there's a 'datavalue' for an entity or property, record that ID
            if ('datatype' in data
                and 'datavalue' in data
                and data['datatype'] in ('wikibase-item', 'wikibase-property')):
                ids.add(data['datavalue'])

            # Recursively gather IDs from all sub-values
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
        """Add the labels in the entity dictionary where they are missing. For example, for properties, and entities in claims...

        Args:
            data (dict): Dictionary to process

        Returns:
            dict: The dictionary with the added labels
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
        label_ids = WikidataLabels._gather_labels_ids(claims)

        labels_dict = {}
        for i in range(0, len(label_ids), query_batch):
            temp_dict = WikidataLabels.get_labels_list(label_ids[i:i+query_batch])
            labels_dict = {**labels_dict, **temp_dict}

        claims = WikidataLabels._add_labels_to_claims(claims, labels_dict=labels_dict)
        return claims

    @staticmethod
    def clean_entity(entity):
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
        Check if a Wikidata item has a corresponding Wikipedia entry.

        Parameters:
        - item: A dictionary representing the Wikidata item.

        Returns:
        - True if the item has a corresponding Wikipedia entry, False otherwise.
        """
        if ('sitelinks' in entity):
            for s in entity['sitelinks']:
                if s.endswith('wiki'):
                    return True
        return False

Base.metadata.create_all(engine)