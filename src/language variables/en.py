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

def merge_entity_description(label, description, aliases):
    text = f"{label}, {description}"

    if len(aliases) > 0:
        text += f", also known as {', '.join(aliases)}"

    return text

def merge_entity_text(label, description, aliases, properties):
    """
    Combines the entity description and its claims into a single text string.

    Parameters:
    - entity_description: A string representing the entity's label, description, and aliases.
    - properties: A list of strings representing the claims of the entity.

    Returns:
    - A string representation of the entity, its description, label, aliases, and its claims. If there are no claims, the description ends with a period.
    """
    text = merge_entity_description(label, description, aliases)

    if len(properties) > 0:
        properties_text = properties_to_text(properties)
        text = f"{text}. Attributes include: {properties_text}"
    else:
        text = f"{text}."

    return text

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

        text += f"{property_label}: {','.join(qualifier_values)}"
    return text

def properties_to_text(properties):
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
            for claim_value in claim_values:
                if len(claims_text) > 0:
                    claims_text += f",\n"

                claims_text += f"\"{claim_value['value']}"

                qualifiers = claim_value.get('qualifiers', {})
                if len(qualifiers) > 0:
                    claims_text += f"({qualifiers_to_text(qualifiers)})"

                claims_text += f"\""

            properties_text += f'\n- {property_label}: {claims_text}'

    return properties_text