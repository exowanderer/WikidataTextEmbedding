from wikidataDB import WikidataEntity
import requests
import time
import json
from datetime import date, datetime
import re

class WikidataTextifier:
    def __init__(self, with_claim_desc=False, with_claim_aliases=False, with_property_desc=False, with_property_aliases=False, language='en'):
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

        self.language = language
        self.lang_values = {
            'en': {
                'months': ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
                'century': 'th century',
                'millennium': 'th millennium',
                'decade': 's',
                'AD': 'AD',
                'BC': 'BC',
                'years': 'years',
                'ten thousand years': 'ten thousand years',
                'hundred thousand years': 'hundred thousand years',
                'million years': 'million years',
                'tens of millions of years': 'tens of millions of years',
                'hundred million years': 'hundred million years',
                'billion years': 'billion years',
                'novalue': 'no value',
                ',': ',',
                'l"': "\"",
                'r"': "\"",
                "(": "(",
                ")": ")",
                ";": ";"
            },
            'de': {
                'months': ['Jan', 'Feb', 'Mär', 'Apr', 'Mai', 'Jun', 'Jul', 'Aug', 'Sep', 'Okt', 'Nov', 'Dez'],
                'century': '. Jahrhundert',
                'millennium': '. Jahrtausend',
                'decade': 'er Jahre',
                'AD': 'n. Chr.',
                'BC': 'v. Chr.',
                'years': 'Jahre',
                'ten thousand years': 'Zehntausend Jahre',
                'hundred thousand years': 'Hunderttausend Jahre',
                'million years': 'Millionen Jahre',
                'tens of millions of years': 'Zehn Millionen Jahre',
                'hundred million years': 'Hundert Millionen Jahre',
                'billion years': 'Milliarden Jahre',
                'novalue': 'no Wert',
                ',': ',',
                'l"': "„",
                'r"': "“",
                "(": "(",
                ")": ")",
                ";": ";"
            },
            'ar': {
                'months': ['كانون الثاني', 'شباط', 'آذار', 'نيسان', 'أيار', 'حزيران', 'تموز', 'آب', 'أيلول', 'تشرين الأول', 'تشرين الثاني', 'كانون الأول'],
                'century': 'قرن',
                'millennium': 'ألفية',
                'decade': 'عقد',
                'AD': 'م',
                'BC': 'ق.م',
                'years': 'سنوات',
                'ten thousand years': 'عشرة آلاف سنة',
                'hundred thousand years': 'مئات آلاف السنين',
                'million years': 'ملايين السنين',
                'tens of millions of years': 'عشرات الملايين من السنين',
                'hundred million years': 'مئات الملايين من السنين',
                'billion years': 'مليار سنة',
                'novalue': 'لا قيمة',
                ',': '،',
                'l"': "«",
                'r"': "»",
                "(": "(",
                ")": ")",
                ";": "؛"
            }
        }
        assert (self.language in self.lang_values), 'Language not found for time parser'
        self.lang = self.lang_values[self.language]

    def merge_entity_property_text(self, entity_description, properties):
        """
        Combines the entity description and its claims into a single text string.

        Parameters:
        - entity_description: A string representing the entity's label, description, and aliases.
        - properties: A list of strings representing the claims of the entity.

        Returns:
        - A string representation of the entity, its description, label, aliases, and its claims. If there are no claims, the description ends with a period.
        """
        if len(properties) > 0:
            if self.language == 'de':
                entity_text = f"{entity_description}. Attribute umfassen: {''.join(properties)}"
            elif self.language == 'ar':
                entity_text = f"{entity_description}. السمات تتضمن: {''.join(properties)}"
            else:
                entity_text = f"{entity_description}. Attributes include: {''.join(properties)}"
        else:
            entity_text = f"{entity_description}."
        return entity_text

    def aliases_to_text(self, aliases):
        if len(aliases) == 0:
            return ""

        if self.language == 'de':
            return f", auch bekannt als {', '.join(aliases)}"
        elif self.language == 'ar':
            return f"، المعروف أيضًا باسم {'، '.join(aliases)}"
        return f", also known as {', '.join(aliases)}"

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

        entity_description = f"{entity.label}{self.lang[',']} {entity.description}"

        if len(entity.aliases) > 0:
            entity_description += self.aliases_to_text(entity.aliases)

        if as_list:
            return entity_description, properties
        return self.merge_entity_property_text(entity_description, properties)

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
            rank_preferred_found = False

            for c in claim:
                value = self.mainsnak_to_value(c.get('mainsnak', c))
                qualifiers = self.qualifiers_to_text(c.get('qualifiers', {}))
                rank = c.get('rank', 'normal').lower()

                if value:
                    if (not rank_preferred_found) or (rank == 'preferred'):
                        # If there exists values with rank preferred, then we only use those values. Else we use all values.
                        if (not rank_preferred_found) and (rank == 'preferred'):
                            # If we find the first value with rank preferred, we reset p_data and set the flag to true to only include preferred values.
                            rank_preferred_found = True
                            p_data = []

                        if len(qualifiers) > 0:
                            value += f" {self.lang['(']}{qualifiers}{self.lang[')']}"
                        p_data.append(value)

            if len(p_data) > 0:
                property = WikidataEntity.get_entity(pid)
                if property:
                    text = f"\n- {property.label}"
                    if self.with_property_desc:
                        text += f"{self.lang[',']} {property.description}"

                    if self.with_property_aliases and (len(property.aliases) > 0):
                        text += self.aliases_to_text(property.aliases)

                    if len(p_data) > 1:
                        p_data_text = self.lang['r\"'] + self.lang[','] + " \n " + self.lang['l\"']
                        p_data_text = p_data_text.join(p_data)
                    else:
                        p_data_text = p_data[0]

                    text += ": " + self.lang['l"'] + p_data_text + self.lang['r"']
                    properties_text.append(text)
        return properties_text

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
                    text += f"{self.lang[',']} {entity.description}"

                if self.with_claim_aliases and len(entity.aliases) > 0:
                    text += self.aliases_to_text(entity.aliases)
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
                try:
                    return self.quantity_to_text(mainsnak['datavalue']['value'])
                except Exception as e:
                    print(e)
                    return mainsnak['datavalue']['value']['amount']

        elif mainsnak.get('snaktype', '') == 'novalue':
            return self.lang_values[self.language]['novalue']

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
                        text += f" {self.lang[';']} "
                    q_data_text = f"{self.lang[',']} ".join(q_data)
                    text += f"{property.label}: {q_data_text}"
        return text

    def quantity_to_text(self, quantity_data):
        """
        Converts quantity data into a readable text string.

        Parameters:
        - quantity_data: A dictionary that includes a quantity value and an optional unit.

        Returns:
        - A string representation of the quantity and its unit (if available).
        """
        quantity = quantity_data.get('amount')
        unit = quantity_data.get('unit')

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

        month_str = self.lang_values[self.language]['months'][month - 1] if month != 0 else ''
        ad = self.lang_values[self.language]['AD']
        bc = self.lang_values[self.language]['BC']

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
            decade_suffix = self.lang_values[self.language]['decade']
            era = ad if year > 0 else bc
            return f"{abs(decade)}{decade_suffix} {era}"
        elif precision == 7:
            century = (abs(year) - 1) // 100 + 1
            era = ad if year > 0 else bc
            return f"{century}{self.lang_values[self.language]['century']} {era}"
        elif precision == 6:
            millennium = (abs(year) - 1) // 1000 + 1
            era = ad if year > 0 else bc
            return f"{millennium}{self.lang_values[self.language]['millennium']} {era}"
        elif precision == 5:
            tens_of_thousands = abs(year) // 10000
            era = ad if year > 0 else bc
            return f"{tens_of_thousands} {self.lang_values[self.language]['ten thousand years']} {era}"
        elif precision == 4:
            hundreds_of_thousands = abs(year) // 100000
            era = ad if year > 0 else bc
            return f"{hundreds_of_thousands} {self.lang_values[self.language]['hundred thousand years']} {era}"
        elif precision == 3:
            millions = abs(year) // 1000000
            era = ad if year > 0 else bc
            return f"{millions} {self.lang_values[self.language]['million years']} {era}"
        elif precision == 2:
            tens_of_millions = abs(year) // 10000000
            era = ad if year > 0 else bc
            return f"{tens_of_millions} {self.lang_values[self.language]['tens of millions of years']} {era}"
        elif precision == 1:
            hundreds_of_millions = abs(year) // 100000000
            era = ad if year > 0 else bc
            return f"{hundreds_of_millions} {self.lang_values[self.language]['hundred million years']} {era}"
        elif precision == 0:
            billions = abs(year) // 1000000000
            era = ad if year > 0 else bc
            return f"{billions} {self.lang_values[self.language]['billion years']} {era}"
        else:
            raise ValueError(f"Unknown precision value {precision}")

    def data_to_text(self, data, datatype):
        """
        Converts time or quantities into a readable text string.

        Parameters:
        - data: A dictionary from the value parameters in Wikidata.
        - datatype: datatype from Wikidata 'time' or 'quantity'

        Returns:
        - A string representation of the item.
        """
        while True:
            try:
                data = {
                    'action': 'wbformatvalue',
                    'format': 'json',
                    'datavalue': json.dumps(data),
                    'datatype': datatype,
                    'uselang': self.language,
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