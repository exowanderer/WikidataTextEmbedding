from sqlalchemy import Column, Text, Boolean, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TypeDecorator
from sqlalchemy.exc import IntegrityError
import orjson

engine = create_engine('sqlite:///../data/Wikidata/sqlite_enwiki.db')
Base = declarative_base()
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)

class JSONType(TypeDecorator):
    """Custom SQLAlchemy type for JSON storage in SQLite."""
    impl = Text

    def process_bind_param(self, value, dialect):
        if value is not None:
            return orjson.dumps(value, separators=(',', ':'))
        return None

    def process_result_value(self, value, dialect):
        if value is not None:
            return orjson.loads(value)
        return None
    
class WikidataEntity(Base):
    __tablename__ = 'wikidata'

    id = Column(Text, primary_key=True)
    label = Column(Text)
    description = Column(Text)
    claims = Column(JSONType)
    aliases = Column(JSONType)

    def add_bulk_entities(data):
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
                return True
            except Exception as e:
                session.rollback()
                print(e)
        return False

    def add_entity(id, label, description, claims, aliases):
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
                return True
            except Exception as e:
                session.rollback()
                print(f"Error: {e}")
        return False

    def get_entity(id):
        with Session() as session:
            return session.query(WikidataEntity).filter_by(id=id).first()
    
class WikidataID(Base):
    __tablename__ = 'wikidataID'

    id = Column(Text, primary_key=True)
    in_wikipedia = Column(Boolean, default=False)
    is_property = Column(Boolean, default=False)

    def add_bulk_ids(data):
        with Session() as session:
            try:
                session.execute(
                    text(
                        """
                        INSERT INTO wikidataID (id, in_wikipedia, is_property) 
                        VALUES (:id, :in_wikipedia, :is_property)
                        ON CONFLICT(id) DO UPDATE 
                        SET in_wikipedia=excluded.in_wikipedia, is_property=excluded.is_property
                        """
                    ),
                    data
                )
                session.commit()
                return True
            except Exception as e:
                session.rollback()
                print(e)
        return False

    def add_id(id, in_wikipedia=False, is_property=False):
        with Session() as session:
            try:
                new_entry = WikidataID(id=id, in_wikipedia=in_wikipedia, is_property=is_property)
                session.add(new_entry)
                session.commit()
                return True
            except Exception as e:
                session.rollback()
                print(e)
        return False

    def get_id(id):
        with Session() as session:
            return session.query(WikidataID).filter_by(id=id).first()
        
Base.metadata.create_all(engine)