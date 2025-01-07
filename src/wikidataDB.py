from sqlalchemy import Column, Text, Boolean, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator
import json
import os

LANGUAGE = os.getenv("LANGUAGE", 'en')
engine = create_engine(f'sqlite:///../data/Wikidata/sqlite_{LANGUAGE}wiki.db',
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

    def process_bind_param(self, value, dialect):
        if value is not None:
            return json.dumps(value, separators=(',', ':'))
        return None

    def process_result_value(self, value, dialect):
        if value is not None:
            return json.loads(value)
        return None

class WikidataEntity(Base):
    __tablename__ = 'wikidata'

    id = Column(Text, primary_key=True)
    label = Column(Text)
    description = Column(Text)
    claims = Column(JSONType)
    aliases = Column(JSONType)

    @staticmethod
    def add_bulk_entities(data):
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
        Retrieve an entity by ID

        Parameters:
        - id: The unique identifier of the entity to be retrieved.

        Returns:
        - The entity object if found, otherwise None.
        """
        with Session() as session:
            return session.query(WikidataEntity).filter_by(id=id).first()

    @staticmethod
    def normalise_item(item, language='en'):
        """
        Normalize a Wikidata item into a dictionary for storage.

        Parameters:
        - item: A dictionary representing the Wikidata item.
        - language: The language code to use for labels and descriptions (default is 'en').

        Returns:
        - A dictionary containing normalized entity data.
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
        Remove unnecessary keys from a nested data structure before storing.

        Parameters:
        - data: The data structure (dictionary or list) from which keys need to be removed.
        - keys_to_remove: A list of keys to be removed (default is ['hash', 'property', 'numeric-id', 'qualifiers-order']).

        Returns:
        - The data structure with specified keys removed.
        """
        if isinstance(data, dict):
            return {
                key: WikidataEntity._remove_keys(value, keys_to_remove)
                for key, value in data.items() if key not in keys_to_remove
            }
        elif isinstance(data, list):
            return [WikidataEntity._remove_keys(item, keys_to_remove) for item in data]
        else:
            return data

    @staticmethod
    def _get_claims(item):
        """
        Extract claims from a Wikidata item.

        Parameters:
        - item: A dictionary representing the Wikidata item.

        Returns:
        - A dictionary containing the extracted claims.
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
        Extract aliases from a Wikidata item.

        Parameters:
        - item: A dictionary representing the Wikidata item.
        - language: The language code to use for extracting aliases (default is 'en').

        Returns:
        - A list of aliases for the specified language.
        """
        aliases = set()
        if language in item['aliases']:
            aliases = set([x['value'] for x in item['aliases'][language]])
        if 'mul' in item['aliases']:
            aliases = aliases | set([x['value'] for x in item['aliases']['mul']])
        return list(aliases)

    @staticmethod
    def clean_claims_for_storage(claims):
        """
        Cleans Wikidata claims to prepare them for storage in a database.

        Parameters:
        - claims: A dictionary where each key is a property ID (pid) and each value is a list of claim statements related to the property.

        Returns:
        - A dictionary with cleaned claims.
        """
        def clean_item(item):
            if 'datavalue' not in item['mainsnak']:
                return {'type': item['mainsnak']['snaktype']}
            if isinstance(item['mainsnak']['datavalue']['value'], dict):
                value = {'type': item['mainsnak']['datavalue']['type'], **item['mainsnak']['datavalue']['value']}
                if 'entity-type' in value:
                    del value['entity-type']
                return value
            return {'type': item['mainsnak']['datavalue']['type'], 'value': item['mainsnak']['datavalue']['value']}

        cleaned_claims = {
            pid: [clean_item(item) for item in value]
            for pid, value in claims.items()
        }
        return cleaned_claims


class WikidataID(Base):
    __tablename__ = 'wikidataID'

    id = Column(Text, primary_key=True)
    in_wikipedia = Column(Boolean, default=False)
    is_property = Column(Boolean, default=False)

    @staticmethod
    def add_bulk_ids(data):
        """
        Add multiple IDs to the database in bulk. If an ID already exists, update the boolean fields (`in_wikipedia` and `is_property`) to True if either the new or old value is True.
        This ensures that `in_wikipedia` is correctly set if an entity is first found in a claim of another entity in Wikipedia, but later discovered to also exist in Wikipedia itself.

        Parameters:
        - data: A list of dictionaries containing ID data to be added or updated.

        Returns:
        - True if the operation was successful, False otherwise.
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
        Add a single ID to the database.

        Parameters:
        - id: The unique identifier for the entity.
        - in_wikipedia: Boolean indicating if the entity is in Wikipedia (default is False).
        - is_property: Boolean indicating if the entity is a property (default is False).

        Returns:
        - True if the operation was successful, False otherwise.
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
        Retrieve an ID from the database.

        Parameters:
        - id: The unique identifier of the ID to be retrieved.

        Returns:
        - The ID object if found, otherwise None.
        """
        with Session() as session:
            return session.query(WikidataID).filter_by(id=id).first()

    @staticmethod
    def is_in_wikipedia(item, language='en'):
        """
        Check if a Wikidata item has a corresponding Wikipedia entry.

        Parameters:
        - item: A dictionary representing the Wikidata item.
        - language: The language code to check for (default is 'en').

        Returns:
        - True if the item has a corresponding Wikipedia entry, False otherwise.
        """
        condition = ('sitelinks' in item) and (f'{language}wiki' in item['sitelinks']) # Has an Wikipedia Sitelink
        condition = condition and ((language in item['labels']) or ('mul' in item['labels'])) # Has a label with the corresponding language or multiligual
        condition = condition and ((language in item['descriptions']) or ('mul' in item['descriptions'])) # Has a description with the corresponding language or multiligual
        return condition

    @staticmethod
    def extract_entity_ids(item, language='en'):
        """
        Extract entity IDs from a Wikidata item, including IDs of entities and properties found in claims and qualifiers as well as IDs of entities as units in quantity datatype.

        Parameters:
        - item: A dictionary representing the Wikidata item.
        - language: The language code to use for extracting data (default is 'en').

        Returns:
        - A list of dictionaries containing entity IDs and their properties.
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

Base.metadata.create_all(engine)