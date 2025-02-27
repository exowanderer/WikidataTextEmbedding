import requests

def remove_keys(data, keys_to_remove):
    if isinstance(data, dict):
        data = {key: remove_keys(value, keys_to_remove) for key, value in data.items() if key not in keys_to_remove}
    elif isinstance(data, list):
        data = [remove_keys(item, keys_to_remove) for item in data]
    return data

def clean_datavalue(data):
    if isinstance(data, dict):
        if len(data.keys()) == 1:
            data = clean_datavalue(data[list(data.keys())[0]])
        else:
            data = {key: clean_datavalue(value) for key, value in data.items()}
    elif isinstance(data, list):
        data = [clean_datavalue(item) for item in data]
    return data

def get_labels(qid):
    try:
        r = requests.get(f"https://www.wikidata.org/w/api.php?action=wbgetentities&ids={qid}&format=json")
        entity = r.json()
        return entity['entities'][qid]['labels']
    except:
        print(qid)
        print(r.text)

def add_labels(data):
    if isinstance(data, dict):
        if 'property' in data:
            data = {
                **data,
                'property-labels': get_labels(data['property'])
            }
        if ('unit' in data) and (data['unit'] != '1'):
            data = {
                **data,
                'unit-labels': get_labels(data['unit'].split('/')[-1])
            }
        if ('datatype' in data) and ('datavalue' in data) and ((data['datatype'] == 'wikibase-item') or (data['datatype'] == 'wikibase-property')):
            data['datavalue'] = {
                'id': data['datavalue'],
                'labels': get_labels(data['datavalue'])
            }

        data = {key: add_labels(value) for key, value in data.items()}
    elif isinstance(data, list):
        data = [add_labels(item) for item in data]
    return data

data = []
for QID in ['Q2']:
    r = requests.get(f"https://www.wikidata.org/wiki/Special:EntityData/{QID}.json")
    entity = r.json()['entities'][QID]

    clean_claims = remove_keys(entity['claims'], ['hash', 'snaktype', 'type', 'entity-type', 'numeric-id', 'qualifiers-order', 'snaks-order'])
    clean_claims = clean_datavalue(clean_claims)
    clean_claims = remove_keys(clean_claims, ['id'])
    clean_claims = add_labels(clean_claims)

    clean_entity = {
        'id': entity['id'],
        'labels': entity['labels'],
        'descriptions': entity['descriptions'],
        'aliases': entity['aliases'],
        'sitelinks': remove_keys(entity['sitelinks'], ['badges']),
        'claims': clean_claims
    }
    data.append(clean_entity)

data