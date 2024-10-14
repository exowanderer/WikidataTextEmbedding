from datetime import datetime, date
import re
from transformers import AutoModel
from typing import List
from wikidataDB import WikidataEntity

class WikidataEmbed:
    def __init__():
        pass

    def entity_to_text(entity, with_desc=False):
        """
        Converts a Wikidata entity to a readable text string, including its label, description,
        and aliases, as well as a list of its properties.

        Parameters:
        - entity: A WikidataEntity object that contains information about the entity.
        - with_desc: Whether to include the entity descriptions in the output text.

        Returns:
        - A string representation of the entity, its description, and its properties.
        """
        properties = WikidataEmbed.properties_to_text(entity.claims, with_desc=with_desc)
        text = f"{entity.label}, {entity.description}"
        text += (f", also known as {', '.join(entity.aliases)}" if (len(entity.aliases) > 0) else "")
        text += (f". Attributes include: {properties}" if (len(properties) > 0) else ".")
        return text

    def mainsnak_to_value(mainsnak, with_desc=False):
        """
        Converts a Wikidata mainsnak to a readable value. A mainsnak is a part of a claim and
        stores the actual value of the statement.
        Datatypes that are kept include: wikibase-item, wikibase-property, monolingualtext, string, time, and quantity

        Parameters:
        - mainsnak: The snak object that contains the value and datatype information.
        - with_desc: Whether to include the description of the value in the output text.

        Returns:
        - A string representation of the value or None if the value cannot be parsed.
        """
        if mainsnak.get('snaktype', '') == 'value':
            if (mainsnak.get('datatype', '') == 'wikibase-item') or (mainsnak.get('datatype', '') == 'wikibase-property'):
                entity_id = mainsnak['datavalue']['value']['id']
                entity = WikidataEntity.get_entity(entity_id)
                if entity is None:
                    return None

                text = entity.label
                if with_desc:
                    text += f", {entity.description}"
                return text

            elif mainsnak.get('datatype', '') == 'monolingualtext':
                return mainsnak['datavalue']['value']['text']

            elif mainsnak.get('datatype', '') == 'string':
                return mainsnak['datavalue']['value']

            elif mainsnak.get('datatype', '') == 'time':
                return WikidataEmbed.time_to_text(mainsnak['datavalue']['value'])

            elif mainsnak.get('datatype', '') == 'quantity':
                text = mainsnak['datavalue']['value']['amount']
                unit = '1'
                if unit != '1':
                    text += f" {unit}"
                return text

        elif mainsnak.get('snaktype', '') == 'novalue':
            return 'no value'

        return None

    def qualifiers_to_text(qualifiers):
        """
        Converts a list of qualifiers to a readable text string.
        Qualifiers provide additional information about a claim.

        Parameters:
        - qualifiers: A dictionary of qualifiers with property IDs as keys and their values as lists.

        Returns:
        - A string representation of the qualifiers.
        """
        text = ""
        for pid, qualifier in qualifiers.items():
            q_data = []

            for q in qualifier:
                value = WikidataEmbed.mainsnak_to_value(q, with_desc=False)
                if value:
                    q_data.append(value)

            if len(q_data) > 0:
                property = WikidataEntity.get_entity(pid)
                if property:
                    if len(text) > 0:
                        text += ' \t '
                    text += f"{property.label}: {', '.join(q_data)}"
        return text


    def properties_to_text(properties, with_desc=False):
        """
        Converts a list of properties (claims) to a readable text string.

        Parameters:
        - properties: A dictionary of properties (claims) with property IDs as keys.
        - with_desc: Whether to include descriptions of the properties in the output.

        Returns:
        - A string representation of the properties and their values.
        """
        text = ""
        for pid, claim in properties.items():
            p_data = []

            for c in claim:
                value = WikidataEmbed.mainsnak_to_value(c.get('mainsnak', c), with_desc=with_desc)
                qualifiers = WikidataEmbed.qualifiers_to_text(c.get('qualifiers', {}))
                if value:
                    if len(qualifiers) > 0:
                        value += f" ({qualifiers})"
                    p_data.append(value)

            if len(p_data) > 0:
                property = WikidataEntity.get_entity(pid)
                if property:
                    text += f"\n- {property.label}"
                    if with_desc:
                        text += f", {property.description}"

                    if len(p_data) > 1:
                        text += f": - {'\n \t- '.join(p_data)}"
                    else:
                        text += f": {p_data[0]}"
        return text

    def quantity_to_text(quantity_data):
        """
        Converts quantity data into a readable text string.

        Parameters:
        - quantity_data: A dictionary that includes a quantity value and an optional unit.

        Returns:
        - A string representation of the quantity and its unit (if available).
        """
        quantity = quantity_data.get('quantity')
        unit = quantity_data.get('unit')

        if unit == '1':
            unit = None
        else:
            unit_qid = unit.rsplit('/')[1]
            entity = WikidataEntity.get_entity(unit_qid)
            if entity:
                unit = entity.label

        return quantity + (f" {unit}" if unit else "")

    def time_to_text(time_data):
        """
        Converts time data into a readable text string.

        Parameters:
        - time_data: A dictionary that includes the time and other related information.

        Returns:
        - A string representation of the time.
        """
        time_value = time_data['time']
        precision = time_data['precision']
        calendarmodel = time_data.get('calendarmodel', 'http://www.wikidata.org/entity/Q1985786')

        # Use regex to parse the time string
        pattern = r'([+-])(\d{1,16})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z'
        match = re.match(pattern, time_value)

        if not match:
            raise ValueError("Malformed time string")

        sign, year, month, day, hour, minute, second = match.groups()
        year = int(year) * (1 if sign == '+' else -1)

        # Convert Julian to Gregorian if necessary
        if 'Q1985786' in calendarmodel and year > 1 and len(str(abs(year))) <= 4:  # Julian calendar
            try:
                month = 1 if month == '00' else int(month)
                day = 1 if day == '00' else int(day)
                julian_date = date(year, int(month), int(day))
                gregorian_date = julian_date.toordinal() + (datetime(1582, 10, 15).toordinal() - datetime(1582, 10, 4).toordinal())
                gregorian_date = date.fromordinal(gregorian_date)
                year, month, day = gregorian_date.year, gregorian_date.month, gregorian_date.day
            except ValueError:
                raise ValueError("Invalid date for Julian calendar")

        # Format the output based on precision
        month_str = datetime.strptime(str(month), '%m').strftime('%b') if month != '00' else ''
        if precision == 14:
            return f"{year} {month_str} {day} {hour}:{minute}:{second}"
        elif precision == 13:
            return f"{year} {month_str} {day} {hour}:{minute}"
        elif precision == 12:
            return f"{year} {month_str} {day} {hour}:00"
        elif precision == 11:
            return f"{year} {month_str} {day}"
        elif precision == 10:
            return f"{year} {month_str}"
        elif precision == 9:
            return f"{year} {'CE' if year > 0 else 'BCE'}"
        elif precision == 8:
            decade = (year // 10) * 10
            return f"{decade}s {'CE' if year > 0 else 'BCE'}"
        elif precision == 7:
            century = (year // 100) + 1 if year > 0 else (year // 100)
            return f"{abs(century)}th century {'CE' if year > 0 else 'BCE'}"
        elif precision == 6:
            millennium = (year // 1000) + 1 if year > 0 else (year // 1000)
            return f"{abs(millennium)}th millennium {'CE' if year > 0 else 'BCE'}"
        elif precision == 5:
            return f"{abs(year) // 10_000} ten thousand years {'CE' if year > 0 else 'BCE'}"
        elif precision == 4:
            return f"{abs(year) // 100_000} hundred thousand years {'CE' if year > 0 else 'BCE'}"
        elif precision == 3:
            return f"{abs(year) // 1_000_000} million years {'CE' if year > 0 else 'BCE'}"
        elif precision == 2:
            return f"{abs(year) // 10_000_000} tens of millions of years {'CE' if year > 0 else 'BCE'}"
        elif precision == 1:
            return f"{abs(year) // 100_000_000} hundred million years {'CE' if year > 0 else 'BCE'}"
        elif precision == 0:
            return f"{abs(year) // 1_000_000_000} billion years {'CE' if year > 0 else 'BCE'}"
        else:
            raise ValueError(f"Unknown precision value {precision}")

class JinaAIEmbeddings:
    def __init__(self, passage_task="retrieval.passage", query_task="retrieval.query", embedding_dim=1024):
        self.model = AutoModel.from_pretrained("jinaai/jina-embeddings-v3", trust_remote_code=True).to('cuda')
        self.passage_task = passage_task
        self.query_task = query_task
        self.embedding_dim = embedding_dim

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        return self.model.encode(texts, task=self.passage_task, truncate_dim=self.embedding_dim)

    def embed_query(self, query: str) -> List[float]:
        return self.model.encode([query], task=self.query_task, truncate_dim=self.embedding_dim)[0]