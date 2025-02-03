""" English """

language = 'en'

novalue = 'no value'

time_variables = {
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
}

def merge_entity_text(label, description, aliases, properties):
    """
    Combines the entity attributes (label, description, aliases, and properties) into a single text string.

    Parameters:
    - label: A string representing the entity's label.
    - description: A string representing the entity's description.
    - aliases: A dictionary of aliases.
    - properties: A dictionary of properties.

    Returns:
    - A string representation of the entity, its description, label, aliases, and its claims. If there are no claims, the description ends with a period.
    """
    import json

    properties = compress_json(properties)
    text = json.dumps({
                'label': label,
                'description': description,
                'aliases': aliases,
                **properties
            }, ensure_ascii=False)

    return text

def compress_json(data):
    cleaned_data = {}  # New dictionary to store cleaned data

    # Iterate through the items of the data
    for key, items in data.items():
        cleaned_items = []
        for item in items:
            qualifiers = {k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in item['qualifiers'].items()}
            clean_item = {'value': item['value'], **qualifiers}
            if len(clean_item) == 1:
                clean_item = clean_item['value']

            cleaned_items.append(clean_item)

        if len(cleaned_items) == 1:
            cleaned_items = cleaned_items[0]
        cleaned_data[key] = cleaned_items

    return cleaned_data

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
    for property_label, qualifier_values in qualifiers.items():
        if len(text) > 0:
            text += f" ; "

        text += f"{property_label}: {', '.join(qualifier_values)}"
    return text

def properties_to_text(properties, label=""):
    """
    Converts a list of properties (claims) to a readable text string.

    Parameters:
    - properties: A dictionary of properties (claims) with property IDs as keys.

    Returns:
    - A string representation of the properties and their values.
    """
    properties_text = ""
    for property_label, claim_values in properties.items():
        if len(claim_values) > 0:

            claims_text = ""
            qualifier_exists = any([len(claim_value.get('qualifiers', {})) > 0 for claim_value in claim_values])
            if qualifier_exists:
                for claim_value in claim_values:
                    if len(claims_text) > 0:
                        claims_text += f"\n"

                    claims_text += f"{label}: {property_label}: {claim_value['value']}"

                    qualifiers = claim_value.get('qualifiers', {})
                    if len(qualifiers) > 0:
                        claims_text += f" ({qualifiers_to_text(qualifiers)})"
            else:
                claims_text = ', '.join([claim_value['value'] for claim_value in claim_values])
                claims_text = f"{label}: {property_label}: {claims_text}"

            properties_text += f'\n{claims_text}'

    return properties_text