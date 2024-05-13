from tqdm import trange
import bz2
import json
import requests

from sentence_transformers import SentenceTransformer
from tqdm import tqdm
from urllib.request import urlopen


def embedd_jina_api(statement):

    url = 'https://api.jina.ai/v1/embeddings'

    headers = {
        'Content-Type': 'application/json',
        'Authorization': (
            'Bearer '
            'jina_a5115787a3624a52a1841a5c90bda2d494No-PfR74durwpOSX0waSUjI02m'
        )
    }

    data = {
        'input': [statement],
        'model': 'jina-embeddings-v2-base-en'
    }

    response = requests.post(url, headers=headers, json=data)
    return json.loads(
        response.content.decode('utf-8')
    )['data'][0]['embedding']


if 'embedder' not in locals():
    embedder = SentenceTransformer(
        "jinaai/jina-embeddings-v2-base-en",
        trust_remote_code=True
    )

path = 'https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2'

# t = trange(desc='Bar desc', leave=True)
# for i in t:
#     t.set_description("Bar desc (file %i)" % i)
#     t.refresh() # to show immediately the update
#     sleep(0.01)

n_attempts = n_attempts if 'n_attempts' in locals() else 0
n_success = n_success if 'n_success' in locals() else 0
n_has_sitelinks = n_has_sitelinks if 'n_has_sitelinks' in locals() else 0
n_has_datavalue = n_has_datavalue if 'n_has_datavalue' in locals() else 0
n_has_en_desc = n_has_en_desc if 'n_has_en_desc' in locals() else 0

dict_vecdb = []
with urlopen(path) as stream:
    with bz2.BZ2File(stream) as file:
        pbar = tqdm(enumerate(file))
        for k_iter, line in pbar:
            pbar_desc = (
                f'Counters: '
                f'n_attempts {n_attempts} '
                # f'n_statements {n_success}: '
                # f'{n_success/(n_attempts+1):0.1f}/item '
                # f'n_has_sitelinks {n_has_sitelinks}: '
                # f'{n_has_sitelinks/(n_success+1)*100:0.1f}% '
                # f'n_has_datavalue {n_has_sitelinks}: '
                # f'{n_has_datavalue/(n_success+1)*100:0.1f}% '
                # f'n_has_en_desc {n_has_sitelinks}: '
                # f'{n_has_en_desc/(n_success+1)*100:0.1f}% '
            )

            pbar.set_description(pbar_desc)
            pbar.refresh()  # to show immediately the update

            if k_iter < n_attempts:
                continue

            n_attempts = n_attempts + 1

            line = line.decode().strip()

            if line in {'[', ']'}:
                continue

            if line.endswith(','):
                line = line[:-1]

            entity = json.loads(line)
            if 'sitelinks' not in entity.keys():
                continue

            # n_has_sitelinks = n_has_sitelinks + 1

            qid_ = entity['id']
            for prop_claims_ in entity['claims'].items():
                pid_, claimlist_ = prop_claims_
                for claim_ in claimlist_:
                    # print(claim_['mainsnak']['datavalue']['value'])
                    # print(claim_['mainsnak'].keys())

                    item_desc_ = None  # Default to None
                    if 'en' in entity['descriptions'].keys():
                        # n_has_en_desc = n_has_en_desc + 1
                        item_desc_ = entity['descriptions']['en']['value']
                    else:
                        continue

                    value_ = None  # Default to None
                    statement_ = None  # Default to None
                    if 'datavalue' in claim_['mainsnak'].keys():
                        # n_has_datavalue = n_has_datavalue + 1

                        # print('has datavalue', claim_)  # ['mainsnak']
                        value_ = claim_['mainsnak']['datavalue']['value']

                        if isinstance(value_, dict):
                            # print(value)
                            if 'id' in value_:
                                value_ = value_['id']
                            if 'amount' in value_:
                                value_ = value_['amount']
                            if 'time' in value_:
                                value_ = value_['time']

                        statement_ = f'{qid_} {pid_} {value_}'

                        embedding_ = None
                        # if embedder is not None:
                        #     # embedding_ = embedd_jina_api(statement_)
                        #     embedding_ = embedder.encode(statement_)

                        vecdb_line_ = {
                            'qid': qid_,
                            'pid': pid_,
                            'value': value_,
                            'item_label': item_desc_,
                            'property_label': pid_,
                            'value_content': value_,
                            'statement': statement_,
                            'embedding': embedding_
                        }
                        dict_vecdb.append(vecdb_line_)
                        # print('')
                        # n_success = n_success + 1
