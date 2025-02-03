""" Deutsch """

language = 'de'

novalue = 'kein Wert'

time_variables = {
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
    'billion years': 'Milliarden Jahre'
}

def merge_entity_text(label, description, aliases, properties):
    """
    Kombiniert die Entitätsattribute (Label, Beschreibung, Aliase und Eigenschaften) zu einem einzigen Textstring.

    Parameter:
    - label: Ein String, der das Label der Entität darstellt.
    - description: Ein String, der die Beschreibung der Entität darstellt.
    - aliases: Ein Dictionary der Aliase.
    - properties: Ein Dictionary der Eigenschaften.

    Rückgabe:
    - Ein String, der die Entität, ihre Beschreibung, das Label, die Aliase und ihre Ansprüche darstellt. Falls keine Ansprüche vorhanden sind, endet die Beschreibung mit einem Punkt.
    """
    text = f"{label}, {description}"

    if len(aliases) > 0:
        text += f", auch bekannt als {', '.join(aliases)}"

    if len(properties) > 0:
        properties_text = properties_to_text(properties)
        text = f"{text}. Attribute umfassen: {properties_text}"
    else:
        text = f"{text}."

    return text

def qualifiers_to_text(qualifiers):
    """
    Konvertiert eine Liste von Qualifikatoren in einen lesbaren Textstring.
    Qualifikatoren bieten zusätzliche Informationen zu einem Anspruch.

    Parameter:
    - qualifiers: Ein Dictionary von Qualifikatoren mit Eigenschafts-IDs als Schlüsseln und deren Werten als Listen.

    Rückgabe:
    - Ein String, der die Qualifikatoren darstellt.
    """
    text = ""
    for property_label, qualifier_values in qualifiers.items():
        if len(text) > 0:
            text += f" ; "

        text += f"{property_label}: {', '.join(qualifier_values)}"
    return text

def properties_to_text(properties):
    """
    Konvertiert eine Liste von Eigenschaften (Ansprüchen) in einen lesbaren Textstring.

    Parameter:
    - properties: Ein Dictionary von Eigenschaften (Ansprüchen) mit Eigenschafts-IDs als Schlüsseln.

    Rückgabe:
    - Ein String, der die Eigenschaften und ihre Werte darstellt.
    """
    properties_text = ""
    for property_label, claim_values in properties.items():
        if len(claim_values) > 0:

            claims_text = ""
            for claim_value in claim_values:
                if len(claims_text) > 0:
                    claims_text += f",\n "

                claims_text += f"„{claim_value['value']}"

                qualifiers = claim_value.get('qualifiers', {})
                if len(qualifiers) > 0:
                    claims_text += f" ({qualifiers_to_text(qualifiers)})"

                claims_text += f"“"

            properties_text += f'\n- {property_label}: {claims_text}'

    return properties_text