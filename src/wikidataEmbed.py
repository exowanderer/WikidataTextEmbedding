from datetime import datetime, date
import re
from typing import List
from wikidataDB import WikidataEntity
from transformers import AutoModel, AutoTokenizer
import torch

class WikidataTextifier:
    def __init__(self, with_claim_desc=False, with_claim_aliases=False, with_property_desc=False, with_property_aliases=False):
        """
        Initializes the WikidataTextifier with options to include descriptions and aliases for both entities and properties.

        Parameters:
        - with_claim_desc: Whether to include the descriptions of entities in claims in the output text.
        - with_claim_aliases: Whether to include the aliases of entities in claims in the output text.
        - with_property_desc: Whether to include the descriptions of claim properties in the output text.
        - with_property_aliases: Whether to include the aliases of claim properties in the output text.
        """
        self.with_claim_desc = with_claim_desc
        self.with_claim_aliases = with_claim_aliases
        self.with_property_desc = with_property_desc
        self.with_property_aliases = with_property_aliases

    def merge_entity_property_text(self, entity_description, properties):
        """
        Combines the entity description and its claims into a single text string.

        Parameters:
        - entity_description: A string representing the entity's label, description, and aliases.
        - properties: A list of strings representing the claims of the entity.

        Returns:
        - A string representation of the entity, its description, label, aliases, and its claims. If there are no claims, the description ends with a period.
        """
        entity_text = entity_description + f'. Attributes include: {("".join(properties) if (len(properties) > 0) else ".")}'
        return entity_text

    def entity_to_text(self, entity, as_list=False):
        """
        Converts a Wikidata entity into a readable text string, including its label, description, aliases, and claims.

        Parameters:
        - entity: A WikidataEntity object that contains information about the entity.
        - as_list: If True, returns the entity description and a list of claim strings separately. If False, returns a combined text string.

        Returns:
        - If as_list is False: A string representing the entity, its description, label, aliases, and its claims.
        - If as_list is True:
            - entity_description: A string representing the entity's label, description, and aliases.
            - properties: A list of strings representing the entity's claims.
        """
        properties = self.properties_to_text(entity.claims)
        entity_description = f"{entity.label}, {entity.description}"
        entity_description += (f", also known as {', '.join(entity.aliases)}" if (len(entity.aliases) > 0) else "")

        if as_list:
            return entity_description, properties
        return self.merge_entity_property_text(entity_description, properties)

    def mainsnak_to_value(self, mainsnak):
        """
        Converts a Wikidata mainsnak to a readable value. A mainsnak is a part of a claim and
        stores the actual value of the statement.
        Datatypes that are kept include: wikibase-item, wikibase-property, monolingualtext, string, time, and quantity

        Parameters:
        - mainsnak: The snak object that contains the value and datatype information.

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
                if self.with_claim_desc:
                    text += f", {entity.description}"

                if self.with_claim_aliases:
                    text += (f", also known as {', '.join(entity.aliases)}" if (len(entity.aliases) > 0) else "")
                return text

            elif mainsnak.get('datatype', '') == 'monolingualtext':
                return mainsnak['datavalue']['value']['text']

            elif mainsnak.get('datatype', '') == 'string':
                return mainsnak['datavalue']['value']

            elif mainsnak.get('datatype', '') == 'time':
                try:
                    return self.time_to_text(mainsnak['datavalue']['value'])
                except Exception as e:
                    print(e)
                    return mainsnak['datavalue']['value']["time"]

            elif mainsnak.get('datatype', '') == 'quantity':
                text = mainsnak['datavalue']['value']['amount']
                unit = '1'
                if unit != '1':
                    text += f" {unit}"
                return text

        elif mainsnak.get('snaktype', '') == 'novalue':
            return 'no value'

        return None

    def qualifiers_to_text(self, qualifiers):
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
                value = self.mainsnak_to_value(q)
                if value:
                    q_data.append(value)

            if len(q_data) > 0:
                property = WikidataEntity.get_entity(pid)
                if property:
                    if len(text) > 0:
                        text += ' ; '
                    text += f"{property.label}: {', '.join(q_data)}"
        return text


    def properties_to_text(self, properties):
        """
        Converts a list of properties (claims) to a readable text string.

        Parameters:
        - properties: A dictionary of properties (claims) with property IDs as keys.

        Returns:
        - A string representation of the properties and their values.
        """
        properties_text = []
        for pid, claim in properties.items():
            p_data = []

            for c in claim:
                value = self.mainsnak_to_value(c.get('mainsnak', c))
                qualifiers = self.qualifiers_to_text(c.get('qualifiers', {}))
                if value:
                    if len(qualifiers) > 0:
                        value += f" ({qualifiers})"
                    p_data.append(value)

            if len(p_data) > 0:
                property = WikidataEntity.get_entity(pid)
                if property:
                    text = f"\n- {property.label}"
                    if self.with_property_desc:
                        text += f", {property.description}"

                    if self.with_property_aliases:
                        text += (f", also known as {', '.join(property.aliases)}" if (len(property.aliases) > 0) else "")

                    if len(p_data) > 1:
                        p_data_text = ('", \n "'.join(p_data))
                    else:
                        p_data_text = p_data[0]
                    text += f': "{p_data_text}"'
                    properties_text.append(text)
        return properties_text

    def quantity_to_text(self, quantity_data):
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

    def time_to_text(self, time_data):
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
            return f"{year}{'' if year > 0 else ' BC'}"
        elif precision == 8:
            decade = (year // 10) * 10
            return f"{decade}s {'AD' if year > 0 else 'BC'}"
        elif precision == 7:
            century = (year // 100) + 1 if year > 0 else (year // 100)
            return f"{abs(century)}th century {'AD' if year > 0 else 'BC'}"
        elif precision == 6:
            millennium = (year // 1000) + 1 if year > 0 else (year // 1000)
            return f"{abs(millennium)}th millennium {'AD' if year > 0 else 'BC'}"
        elif precision == 5:
            return f"{abs(year) // 10_000} ten thousand years {'AD' if year > 0 else 'BC'}"
        elif precision == 4:
            return f"{abs(year) // 100_000} hundred thousand years {'AD' if year > 0 else 'BC'}"
        elif precision == 3:
            return f"{abs(year) // 1_000_000} million years {'AD' if year > 0 else 'BC'}"
        elif precision == 2:
            return f"{abs(year) // 10_000_000} tens of millions of years {'AD' if year > 0 else 'BC'}"
        elif precision == 1:
            return f"{abs(year) // 100_000_000} hundred million years {'AD' if year > 0 else 'BC'}"
        elif precision == 0:
            return f"{abs(year) // 1_000_000_000} billion years {'AD' if year > 0 else 'BC'}"
        else:
            raise ValueError(f"Unknown precision value {precision}")

    def clean_claims_for_storage(self, claims):
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

    def chunk_text(self, entity, tokenizer, max_length=500):
        """
        Chunks a text into smaller pieces if the token length exceeds the model's maximum input length.

        Parameters:
        - entity: The entity containing the text to be chunked.
        - textifier: A WikidataTextifier instance that helps convert the entity and its properties into text.

        Returns:
        - A list of text chunks that fit within the model's maximum token length.
        """
        entity_description, properties = self.entity_to_text(entity, as_list=True)
        entity_text = self.merge_entity_property_text(entity_description, properties)
        max_length = max_length

        # If the full text does not exceed the maximum tokens then we only return 1 chunk.
        tokens = tokenizer(entity_text, add_special_tokens=False, return_offsets_mapping=True)
        if len(tokens['input_ids']) < max_length:
            return [entity_text]

        # If the label and description already exceed the maximum tokens then we will truncate it and will not include chunks that include claims.
        tokens = tokenizer(entity_description, add_special_tokens=False, return_offsets_mapping=True)
        token_ids, offsets = tokens['input_ids'], tokens['offset_mapping']
        if len(token_ids) >= max_length:
            start, end = offsets[0][0], offsets[max_length - 1][1]
            return [entity_text[start:end]]  # Return the truncated portion of the original text

        # Create the chunks assuming the description/label text is smaller than the maximum tokens.
        chunks = []
        chunk_claims = []
        for claim in properties:
            entity_text = self.merge_entity_property_text(entity_description, chunk_claims+[claim])
            tokens = tokenizer(entity_text, add_special_tokens=False, return_offsets_mapping=True)
            token_ids, offsets = tokens['input_ids'], tokens['offset_mapping']

            # Check when including the current claim if we exceed the maximum tokens.
            if len(token_ids) >= max_length:
                start, end = offsets[0][0], offsets[max_length - 1][1]
                chunks.append(entity_text[start:end])
                if len(chunk_claims) == 0:
                    # If we do exceed it but there's no claim previously added to the chunks, then it means the current claim alone exceeds the maximum tokens, and we already included it in a trimmed chunk alone.
                    chunk_claims = []
                else:
                    # Include the claim in a new chunk so where it's information doesn't get trimmed.
                    chunk_claims = [claim]
            else:
                chunk_claims.append(claim)

        if len(chunk_claims) > 0:
            entity_text = self.merge_entity_property_text(entity_description, chunk_claims)
            tokens = tokenizer(entity_text, add_special_tokens=False, return_offsets_mapping=True)
            token_ids, offsets = tokens['input_ids'], tokens['offset_mapping']

            if len(token_ids) >= max_length:
                start, end = offsets[0][0], offsets[max_length - 1][1]
            else:
                start, end = offsets[0][0], offsets[-1][1]
            chunks.append(entity_text[start:end])

        return chunks

class JinaAIEmbeddings:
    def __init__(self, passage_task="retrieval.passage", query_task="retrieval.query", embedding_dim=1024):
        """
        Initializes the JinaAIEmbeddings class with the model, tokenizer, and task identifiers.

        Parameters:
        - passage_task: Task identifier for embedding documents (default: "retrieval.passage").
        - query_task: Task identifier for embedding queries (default: "retrieval.query").
        - embedding_dim: The dimensionality of the embeddings (default: 1024).
        """
        self.passage_task = passage_task
        self.query_task = query_task
        self.embedding_dim = embedding_dim

        self.model = AutoModel.from_pretrained("jinaai/jina-embeddings-v3", trust_remote_code=True).to('cuda')
        self.tokenizer = AutoTokenizer.from_pretrained("jinaai/jina-embeddings-v3", trust_remote_code=True)

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Generates embeddings for a list of documents (passages).

        Parameters:
        - texts: A list of document strings to embed.

        Returns:
        - A list of embeddings, each corresponding to a document, with a dimensionality specified by embedding_dim.
        """
        with torch.no_grad():
            return self.model.encode(texts, task=self.passage_task, truncate_dim=self.embedding_dim)

    def embed_query(self, text: str) -> List[float]:
        """
        Generates an embedding for a single query.

        Parameters:
        - query: The query string to embed.

        Returns:
        - A single embedding as a list of floats with a dimensionality specified by embedding_dim.
        """
        with torch.no_grad():
            return self.model.encode([text], task=self.query_task, truncate_dim=self.embedding_dim)[0]