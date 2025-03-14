from wikidataDB import WikidataEntity
import requests
import time
import json
from datetime import date, datetime
import re
import importlib

class WikidataTextifier:
    def __init__(self, language='en'):
        """
        Initializes the WikidataTextifier with the specified language.

        Parameters:
        - language (str): The language code used by the textifier (default is "en").
        """

        self.language = language
        try:
            # Importing custom functions and variables from a formating python script in the language_variables folder.
            self.langvar = importlib.import_module(f"language_variables.{language}")
        except Exception as e:
            raise ValueError(f"Language file for '{language}' not found.")

    def entity_to_text(self, entity, properties=None):
        """
        Converts a Wikidata entity into a human-readable text string.

        Parameters:
        - entity (WikidataEntity): A WikidataEntity object containing entity data (label, description, claims, etc.).
        - properties (dict or None): A dictionary of properties (claims). If None, the properties will be derived from entity.claims.

        Returns:
        - str: A human-readable representation of the entity, its description, aliases, and claims.
        """
        if properties is None:
            properties = self.properties_to_dict(entity.claims)

        return self.langvar.merge_entity_text(entity.label, entity.description, entity.aliases, properties)

    def properties_to_dict(self, properties):
        """
        Converts a dictionary of properties (claims) into a dict suitable for text generation.

        Parameters:
        - properties (dict): A dictionary of claims keyed by property IDs. 
                             Each value is a list of claim statements for that property.

        Returns:
        - dict: A dictionary mapping property labels to a list of their parsed values (and qualifiers).
        """
        properties_dict = {}
        for pid, claim in properties.items():
            p_data = []
            rank_preferred_found = False

            for c in claim:
                value = self.mainsnak_to_value(c.get('mainsnak', c))
                qualifiers = self.qualifiers_to_dict(c.get('qualifiers', {}))
                rank = c.get('rank', 'normal').lower()

                # Only store "normal" ranks. if one "preferred" rank exists, then only store "preferred" ranks.
                if value:
                    if ((not rank_preferred_found) and (rank == 'normal')) or (rank == 'preferred'):
                        if (not rank_preferred_found) and (rank == 'preferred'):
                            rank_preferred_found = True
                            p_data = []

                        p_data.append({'value': value, 'qualifiers': qualifiers})

            if len(p_data) > 0:
                property = WikidataEntity.get_entity(pid)
                if property:
                    properties_dict[property.label] = p_data

        return properties_dict

    def qualifiers_to_dict(self, qualifiers):
        """
        Converts qualifiers into a dictionary suitable for text generation.

        Parameters:
        - qualifiers (dict): A dictionary of qualifiers keyed by property IDs. 
                             Each value is a list of qualifier statements.

        Returns:
        - dict: A dictionary mapping property labels to a list of their parsed values.
        """
        qualifier_dict = {}
        for pid, qualifier in qualifiers.items():
            q_data = []

            for q in qualifier:
                value = self.mainsnak_to_value(q)
                if value:
                    q_data.append(value)

            if len(q_data) > 0:
                property = WikidataEntity.get_entity(pid)
                if property:
                    qualifier_dict[property.label] = q_data
        return qualifier_dict

    def mainsnak_to_value(self, mainsnak):
        """
        Converts a Wikidata 'mainsnak' object into a human-readable value string. This method interprets various datatypes (e.g., wikibase-item, string, time, quantity) and returns a formatted text representation.

        Parameters:
        - mainsnak (dict): A snak object containing the value and datatype information.

        Returns:
        - str or None: A string representation of the value, or None if parsing fails.
        """
        if mainsnak.get('snaktype', '') == 'value':
            if (mainsnak.get('datatype', '') == 'wikibase-item') or (mainsnak.get('datatype', '') == 'wikibase-property'):
                entity_id = mainsnak['datavalue']['value']['id']
                entity = WikidataEntity.get_entity(entity_id)
                if entity is None:
                    return None

                text = entity.label
                return text

            elif mainsnak.get('datatype', '') == 'monolingualtext':
                return mainsnak['datavalue']['value']['text']

            elif mainsnak.get('datatype', '') == 'string':
                return mainsnak['datavalue']['value']

            elif mainsnak.get('datatype', '') == 'time':
                try:
                    return self.time_to_text(mainsnak['datavalue']['value'])
                except Exception as e:
                    print("Error in time formating:", e)
                    return mainsnak['datavalue']['value']["time"]

            elif mainsnak.get('datatype', '') == 'quantity':
                try:
                    return self.quantity_to_text(mainsnak['datavalue']['value'])
                except Exception as e:
                    print(e)
                    return mainsnak['datavalue']['value']['amount']

        elif mainsnak.get('snaktype', '') == 'novalue':
            return self.langvar.novalue

        return None

    def quantity_to_text(self, quantity_data):
        """
        Converts Wikidata quantity data into a human-readable string.

        Parameters:
        - quantity_data (dict): A dictionary with 'amount' and optionally 'unit' (often a QID).

        Returns:
        - str: A textual representation of the quantity (e.g., "5 kg").
        """
        quantity = quantity_data.get('amount')
        unit = quantity_data.get('unit')

        # 'unit' of '1' means that the value is a count and doesn't require a unit.
        if unit == '1':
            unit = None
        else:
            unit_qid = unit.rsplit('/')[-1]
            entity = WikidataEntity.get_entity(unit_qid)
            if entity:
                unit = entity.label

        return quantity + (f" {unit}" if unit else "")

    def time_to_text(self, time_data):
        """
        Converts Wikidata time data into a human-readable string.

        Parameters:
        - time_data (dict): A dictionary containing the time string, precision, and calendar model.

        Returns:
        - str: A textual representation of the time with appropriate granularity.
        """
        time_value = time_data['time']
        precision = time_data['precision']
        calendarmodel = time_data.get('calendarmodel', 'http://www.wikidata.org/entity/Q1985786')

        # Use regex to parse the time string
        pattern = r'([+-])(\d{1,16})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z'
        match = re.match(pattern, time_value)

        if not match:
            raise ValueError("Malformed time string")

        sign, year_str, month_str, day_str, hour_str, minute_str, second_str = match.groups()
        year = int(year_str) * (1 if sign == '+' else -1)

        # Convert Julian to Gregorian if necessary
        if 'Q1985786' in calendarmodel and year > 1 and len(str(abs(year))) <= 4:  # Julian calendar
            try:
                month = 1 if month_str == '00' else int(month_str)
                day = 1 if day_str == '00' else int(day_str)
                julian_date = date(year, month, day)
                gregorian_ordinal = julian_date.toordinal() + (datetime(1582, 10, 15).toordinal() - datetime(1582, 10, 5).toordinal())
                gregorian_date = date.fromordinal(gregorian_ordinal)
                year, month, day = gregorian_date.year, gregorian_date.month, gregorian_date.day
            except ValueError:
                raise ValueError("Invalid date for Julian calendar")
        else:
            month = int(month_str) if month_str != '00' else 1
            day = int(day_str) if day_str != '00' else 1

        month_str = self.langvar.time_variables['months'][month - 1] if month != 0 else ''
        ad = self.langvar.time_variables['AD']
        bc = self.langvar.time_variables['BC']

        if precision == 14:
            return f"{year} {month_str} {day} {hour_str}:{minute_str}:{second_str}"
        elif precision == 13:
            return f"{year} {month_str} {day} {hour_str}:{minute_str}"
        elif precision == 12:
            return f"{year} {month_str} {day} {hour_str}:00"
        elif precision == 11:
            return f"{day} {month_str} {year}"
        elif precision == 10:
            return f"{month_str} {year}"
        elif precision == 9:
            era = '' if year > 0 else f' {bc}'
            return f"{abs(year)}{era}"
        elif precision == 8:
            decade = (year // 10) * 10
            decade_suffix = self.langvar.time_variables['decade']
            era = ad if year > 0 else bc
            return f"{abs(decade)}{decade_suffix} {era}"
        elif precision == 7:
            century = (abs(year) - 1) // 100 + 1
            era = ad if year > 0 else bc
            return f"{century}{self.langvar.time_variables['century']} {era}"
        elif precision == 6:
            millennium = (abs(year) - 1) // 1000 + 1
            era = ad if year > 0 else bc
            return f"{millennium}{self.langvar.time_variables['millennium']} {era}"
        elif precision == 5:
            tens_of_thousands = abs(year) // 10000
            era = ad if year > 0 else bc
            return f"{tens_of_thousands} {self.langvar.time_variables['ten thousand years']} {era}"
        elif precision == 4:
            hundreds_of_thousands = abs(year) // 100000
            era = ad if year > 0 else bc
            return f"{hundreds_of_thousands} {self.langvar.time_variables['hundred thousand years']} {era}"
        elif precision == 3:
            millions = abs(year) // 1000000
            era = ad if year > 0 else bc
            return f"{millions} {self.langvar.time_variables['million years']} {era}"
        elif precision == 2:
            tens_of_millions = abs(year) // 10000000
            era = ad if year > 0 else bc
            return f"{tens_of_millions} {self.langvar.time_variables['tens of millions of years']} {era}"
        elif precision == 1:
            hundreds_of_millions = abs(year) // 100000000
            era = ad if year > 0 else bc
            return f"{hundreds_of_millions} {self.langvar.time_variables['hundred million years']} {era}"
        elif precision == 0:
            billions = abs(year) // 1000000000
            era = ad if year > 0 else bc
            return f"{billions} {self.langvar.time_variables['billion years']} {era}"
        else:
            raise ValueError(f"Unknown precision value {precision}")

    def data_to_text(self, data, datatype):
        """
        Converts specific Wikidata data (time or quantity) into a string using the Wikidata API. Ideally, this function should replace "time_to_text" and "quantity_to_text", however it's too slow.

        Parameters:
        - data (dict): The dictionary structure of the datavalue (time or quantity).
        - datatype (str): The datatype (usually 'time' or 'quantity').

        Returns:
        - str: The formatted value (as returned by the Wikidata API).
        """
        while True:
            try:
                data = {
                    'action': 'wbformatvalue',
                    'format': 'json',
                    'datavalue': json.dumps(data),
                    'datatype': datatype,
                    'uselang': self.langvar.language,
                    'formatversion': 2
                }
                r = requests.get('https://www.wikidata.org/w/api.php', params=data)
                return r.json()['result']
            except Exception as e:
                print(e)
                while True:
                    try:
                        response = requests.get("https://www.google.com", timeout=5)
                        if response.status_code == 200:
                            break
                    except Exception as e:
                        print("Waiting for internet connection...")
                        time.sleep(5)

    def chunk_text(self, entity, tokenizer, max_length=500):
        """
        Splits a text representation of an entity into smaller chunks so that each chunk fits within the token limit of a given tokenizer.

        Parameters:
        - entity (WikidataEntity): The entity to be textified and chunked.
        - tokenizer: A tokenizer (e.g. from Hugging Face) used to count tokens.
        - max_length (int): The maximum number of tokens allowed per chunk (default is 500).

        Returns:
        - list[str]: A list of text chunks, each within the token limit.
        """
        entity_text = self.entity_to_text(entity)
        max_length = max_length

        # If the full text does not exceed the maximum tokens then we only return 1 chunk.
        tokens = tokenizer(entity_text, add_special_tokens=False, return_offsets_mapping=True)
        if len(tokens['input_ids']) < max_length:
            return [entity_text]

        # If the label and description already exceed the maximum tokens then we will truncate it and will not include chunks that include claims.
        entity_description= self.entity_to_text(entity, properties={})
        tokens = tokenizer(entity_description, add_special_tokens=False, return_offsets_mapping=True)
        token_ids, offsets = tokens['input_ids'], tokens['offset_mapping']
        if len(token_ids) >= max_length:
            start, end = offsets[0][0], offsets[max_length - 1][1]
            return [entity_text[start:end]]  # Return the truncated portion of the original text

        # Create the chunks assuming the description/label text is smaller than the maximum tokens.
        properties = self.properties_to_dict(entity.claims)
        chunks = []
        chunk_claims = {}
        for claim, value in properties.items():
            current_chunk_claims = {**chunk_claims, claim: value}
            entity_text = self.entity_to_text(entity, current_chunk_claims)
            tokens = tokenizer(entity_text, add_special_tokens=False, return_offsets_mapping=True)

            # Check when including the current claim if we exceed the maximum tokens.
            if len(tokens['input_ids']) >= max_length:
                start, end = tokens['offset_mapping'][0][0], tokens['offset_mapping'][max_length - 1][1]
                chunks.append(entity_text[start:end])

                # If we do exceed it but there's no claim previously added to the chunks, then it means the current claim alone exceeds the maximum tokens, and we already included it in a trimmed chunk alone.
                if len(chunk_claims) == 0:
                    chunk_claims = {}

                # Include the claim in a new chunk so where it's information doesn't get trimmed.
                else:
                    chunk_claims = {claim: value}
            else:
                chunk_claims = current_chunk_claims

        # Add the final chunk if any claims remain
        if len(chunk_claims) > 0:
            entity_text = self.entity_to_text(entity, chunk_claims)
            tokens = tokenizer(entity_text, add_special_tokens=False, return_offsets_mapping=True)

            if len(tokens['input_ids']) >= max_length:
                start, end = tokens['offset_mapping'][0][0], tokens['offset_mapping'][max_length - 1][1]
            else:
                start, end = tokens['offset_mapping'][0][0], tokens['offset_mapping'][-1][1]
            chunks.append(entity_text[start:end])

        return chunks