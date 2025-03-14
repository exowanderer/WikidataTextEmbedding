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
            }, ensure_ascii=False, indent=4)

    return text

def compress_json(data):
    cleaned_data = {}  # New dictionary to store cleaned data

    # Iterate through the items of the data
    for key, items in data.items():
        if (items is not None) and (len(items) > 0):
            cleaned_items = []
            for item in items:
                qualifiers = {k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in item['qualifiers'].items()}
                clean_item = {'value': item['value'], **qualifiers}
                if len(clean_item) == 1:
                    clean_item = clean_item['value']

                cleaned_items.append(clean_item)

            if len(cleaned_items) == 1:
                cleaned_items = cleaned_items[0]
            elif len(cleaned_items) > 1:
                cleaned_data[key] = cleaned_items

    return cleaned_data