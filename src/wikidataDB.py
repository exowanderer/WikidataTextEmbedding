from sqlalchemy import Column, Text, Boolean, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator
import json
import os

"""
SQLite database setup for quick entity lookup. A database file is created per language.
"""
LANGUAGE = os.getenv("LANGUAGE", 'en')
engine = create_engine(
    f'sqlite:///../data/Wikidata/sqlite_{LANGUAGE}wiki.db',
    pool_size=5,       # Limit the number of open connections
    max_overflow=10,   # Allow extra connections beyond pool_size
    pool_recycle=10    # Recycle connections every 10 seconds
)
Base = declarative_base()
Session = sessionmaker(bind=engine)

class JSONType(TypeDecorator):
    """Custom SQLAlchemy type for JSON storage in SQLite."""
    impl = Text

    def process_bind_param(self, value, dialect):
        if value is not None:
            return json.dumps(value, separators=(',', ':'))
        return None

    def process_result_value(self, value, dialect):
        if value is not None:
            return json.loads(value)
        return None

class WikidataEntity(Base):
    """Represents a Wikidata entity with label, description, aliases, and claims."""

    __tablename__ = 'wikidata'

    id = Column(Text, primary_key=True)
    label = Column(Text)
    description = Column(Text)
    aliases = Column(JSONType)
    claims = Column(JSONType)

    @staticmethod
    def add_bulk_entities(data):
        """
        Add multiple entities to the database in bulk, if the item already exists then it's ignored.

        Parameters:
        - data (list[dict]): A list of dictionaries representing entities to be added.

        Returns:
        - bool: True if the operation was successful, False otherwise.
        """
        worked = False
        with Session() as session:
            try:
                session.execute(
                    text(
                        """
                        INSERT INTO wikidata (id, label, description, claims, aliases)
                        VALUES (:id, :label, :description, :claims, :aliases)
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
    def add_entity(id, label, description, claims, aliases):
        """
        Add a single Wikidata entity to the database.

        Parameters:
        - id (str): The unique identifier for the entity.
        - label (str): The entity's label.
        - description (str): The entity's description.
        - claims (dict): The entity's claims.
        - aliases (dict): The entity's aliases.

        Returns:
        - bool: True if successful, False otherwise.
        """
        worked = False
        with Session() as session:
            try:
                new_entry = WikidataEntity(
                    id=id,
                    label=label,
                    description=description,
                    claims=claims,
                    aliases=aliases
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
    def get_entity(id):
        """
        Retrieve an entity by its ID.

        Parameters:
        - id (str): The unique identifier of the entity.

        Returns:
        - WikidataEntity or None: The entity object if found, otherwise None.
        """
        with Session() as session:
            return session.query(WikidataEntity).filter_by(id=id).first()

    @staticmethod
    def normalise_item(item, language='en'):
        """
        Normalize a Wikidata item into a dictionary for storage.

        Parameters:
        - item (dict): The raw Wikidata item.
        - language (str): The language code to use for label/description lookup. Default is 'en'.

        Returns:
        - dict: A dictionary containing normalized entity data suitable for insertion.
        """
        label = item['labels'][language]['value'] if (language in item['labels']) else (item['labels']['mul']['value'] if ('mul' in item['labels']) else '') # Take the label from the language, if missing take it from the multiligual class
        description = item['descriptions'][language]['value'] if (language in item['descriptions']) else (item['descriptions']['mul']['value'] if ('mul' in item['descriptions']) else '') # Take the description from the language, if missing take it from the multiligual class
        aliases = WikidataEntity._get_aliases(item, language=language)
        claims = WikidataEntity._get_claims(item)
        return {
            'id': item['id'],
            'label': label,
            'description': description,
            'aliases': json.dumps(aliases, separators=(',', ':')),
            'claims': json.dumps(claims, separators=(',', ':')),
        }

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
            return {key: WikidataEntity._remove_keys(value, keys_to_remove) for key, value in data.items() if key not in keys_to_remove}
        elif isinstance(data, list):
            return [WikidataEntity._remove_keys(item, keys_to_remove) for item in data]
        else:
            return data

    @staticmethod
    def _get_claims(item):
        """
        Extract and clean claims from a Wikidata item.

        Parameters:
        - item (dict): The raw Wikidata item.

        Returns:
        - dict: A dictionary of extracted claims, keyed by property ID.
        """
        claims = {}
        if 'claims' in item:
            for pid,x in item['claims'].items():
                pid_claims = []
                for i in x:
                    if (i['type'] == 'statement') and (i['rank'] != 'deprecated'):
                        pid_claims.append({
                            'mainsnak': WikidataEntity._remove_keys(i['mainsnak']) if 'mainsnak' in i else {},
                            'qualifiers': WikidataEntity._remove_keys(i['qualifiers']) if 'qualifiers' in i else {},
                            'rank': i['rank']
                        })
                if len(pid_claims) > 0:
                    claims[pid] = pid_claims
        return claims

    @staticmethod
    def _get_aliases(item, language='en'):
        """
        Extract aliases from a Wikidata item for a given language, plus any 'mul' entries.

        Parameters:
        - item (dict): The raw Wikidata item.
        - language (str): The language code. Default is 'en'.

        Returns:
        - list[str]: A list of aliases in the specified language (and 'mul' if present).
        """
        aliases = set()
        if language in item['aliases']:
            aliases = set([x['value'] for x in item['aliases'][language]])
        if 'mul' in item['aliases']:
            aliases = aliases | set([x['value'] for x in item['aliases']['mul']])
        return list(aliases)

class WikidataID(Base):
    """ Represents an ID record in the database, indicating whether it appears in Wikipedia or is a property. """

    __tablename__ = 'wikidataID'

    id = Column(Text, primary_key=True)
    in_wikipedia = Column(Boolean, default=False)
    is_property = Column(Boolean, default=False)

    @staticmethod
    def add_bulk_ids(data):
        """
        Add multiple IDs to the database in bulk. If an ID exists, update its boolean fields.

        Parameters:
        - data (list[dict]): A list of dictionaries with 'id', 'in_wikipedia', and 'is_property' fields.

        Returns:
        - bool: True if successful, False otherwise.
        """
        worked = False
        with Session() as session:
            try:
                session.execute(
                    text(
                        """
                        INSERT INTO wikidataID (id, in_wikipedia, is_property)
                        VALUES (:id, :in_wikipedia, :is_property)
                        ON CONFLICT(id) DO UPDATE
                        SET
                            in_wikipedia = CASE WHEN excluded.in_wikipedia = TRUE THEN excluded.in_wikipedia ELSE wikidataID.in_wikipedia END,
                            is_property = CASE WHEN excluded.is_property = TRUE THEN excluded.is_property ELSE wikidataID.is_property END
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
    def add_id(id, in_wikipedia=False, is_property=False):
        """
        Add a single ID record to the database.

        Parameters:
        - id (str): The unique identifier.
        - in_wikipedia (bool): Whether the entity is in Wikipedia. Default is False.
        - is_property (bool): Whether the entity is a property. Default is False.

        Returns:
        - bool: True if successful, False otherwise.
        """
        worked = False
        with Session() as session:
            try:
                new_entry = WikidataID(id=id, in_wikipedia=in_wikipedia, is_property=is_property)
                session.add(new_entry)
                session.commit()
                session.flush()
                worked = True
            except Exception as e:
                session.rollback()
                print(e)
        return worked

    @staticmethod
    def get_id(id):
        """
        Retrieve a record by its ID.

        Parameters:
        - id (str): The unique identifier of the record.

        Returns:
        - WikidataID or None: The record if found, otherwise None.
        """
        with Session() as session:
            return session.query(WikidataID).filter_by(id=id).first()

    @staticmethod
    def is_in_wikipedia(item, language='en'):
        """
        Check if a Wikidata item has a corresponding Wikipedia entry.

        Parameters:
        - item (dict): The Wikidata item.
        - language (str): The Wikipedia language code. Default is 'en'.

        Returns:
        - bool: True if the item has a Wikipedia sitelink and label/description in the specified language or 'mul'.
        """
        condition = ('sitelinks' in item) and (f'{language}wiki' in item['sitelinks']) # Has an Wikipedia Sitelink
        condition = condition and ((language in item['labels']) or ('mul' in item['labels'])) # Has a label with the corresponding language or multiligual
        condition = condition and ((language in item['descriptions']) or ('mul' in item['descriptions'])) # Has a description with the corresponding language or multiligual
        return condition

    @staticmethod
    def extract_entity_ids(item, language='en'):
        """
        Extract entity and property IDs from a Wikidata item (including claims, qualifiers, and units).

        Parameters:
        - item (dict): The Wikidata item.
        - language (str): The language code for additional checks. Default is 'en'.

        Returns:
        - list[dict]: A list of dictionaries with 'id', 'in_wikipedia', and 'is_property' for each discovered ID.
        """
        if item is None:
            return []

        batch_ids = [{'id': item['id'], 'in_wikipedia': WikidataID.is_in_wikipedia(item, language=language), 'is_property': False}]

        for pid,claim in item.get('claims', {}).items():
            batch_ids.append({'id': pid, 'in_wikipedia': False, 'is_property': True})

            for c in claim:
                if ('mainsnak' in c) and ('datavalue' in c['mainsnak']):
                    if (c['mainsnak'].get('datatype', '') == 'wikibase-item'):
                        id = c['mainsnak']['datavalue']['value']['id']
                        batch_ids.append({'id': id, 'in_wikipedia': False, 'is_property': False})

                    elif (c['mainsnak'].get('datatype', '') == 'wikibase-property'):
                        id = c['mainsnak']['datavalue']['value']['id']
                        batch_ids.append({'id': id, 'in_wikipedia': False, 'is_property': True})

                    elif (c['mainsnak'].get('datatype', '') == 'quantity') and (c['mainsnak']['datavalue']['value'].get('unit', '1') != '1'):
                        id = c['mainsnak']['datavalue']['value']['unit'].rsplit('/', 1)[1]
                        batch_ids.append({'id': id, 'in_wikipedia': False, 'is_property': False})

                if 'qualifiers' in c:
                    for pid, qualifier in c['qualifiers'].items():
                        batch_ids.append({'id': pid, 'in_wikipedia': False, 'is_property': True})
                        for q in qualifier:
                            if ('datavalue' in q):
                                if (q['datatype'] == 'wikibase-item'):
                                    id = q['datavalue']['value']['id']
                                    batch_ids.append({'id': id, 'in_wikipedia': False, 'is_property': False})

                                elif(q['datatype'] == 'wikibase-property'):
                                    id = q['datavalue']['value']['id']
                                    batch_ids.append({'id': id, 'in_wikipedia': False, 'is_property': True})

                                elif (q['datatype'] == 'quantity') and (q['datavalue']['value'].get('unit', '1') != '1'):
                                    id = q['datavalue']['value']['unit'].rsplit('/', 1)[1]
                                    batch_ids.append({'id': id, 'in_wikipedia': False, 'is_property': False})
        return batch_ids

# Create tables if they don't already exist.
Base.metadata.create_all(engine)