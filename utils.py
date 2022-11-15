import requests

DEFAULT_TIMEOUT = 30


PROP_IDS = {
    'nationality': 'P27',
    'country': 'P17',
    'instance of': 'P31',
}


def get_entity_info(entity_qid, lang: str = 'en'):
    resp = requests.get('https://www.wikidata.org/w/api.php', params={
        'action': 'wbgetentities',
        'ids': entity_qid,
        'languages': lang,
        'format': 'json'
    }, timeout=DEFAULT_TIMEOUT)
    obj = resp.json()

    keys = ['labels', 'descriptions', 'aliases']
    entity_dict = obj['entities'][entity_qid]
    info_dict = {
        key: entity_dict[key][lang]['value']
        for key in ['labels', 'descriptions']
    }
    info_dict.update({
        'aliases': [a['value'] for a in entity_dict['aliases'][lang]]
    })
    return info_dict


def get_entity_name(entity_qid: str, lang: str = 'en'):
    info_dict = get_entity_info(entity_qid)
    return info_dict['labels']


def get_entity_prop(qid: str, pid: str):
    resp = requests.get('https://www.wikidata.org/w/api.php', params={
        'action': 'wbgetclaims',
        'entity': qid,
        'property': pid,
        'format': 'json'
    }, timeout=DEFAULT_TIMEOUT).json()

    if pid not in resp['claims']:
        return []

    qids = [entry['mainsnak']['datavalue']['value']['id'] for entry in resp['claims'][pid]]
    return qids


def get_entity_type(entity_qid: str, qids_only:bool=False):
    qids = get_entity_prop(entity_qid, PROP_IDS['instance of'])
    if qids_only:
        return qids
    names = [get_entity_name(qid) for qid in qids]
    return zip(qids, names)


def get_nationality(person_qid: str):
    qids = get_entity_prop(person_qid, PROP_IDS['nationality'])
    names = [get_entity_name(qid) for qid in qids]
    return zip(qids, names)


def get_country(location_qid: str):
    qids = get_entity_prop(location_qid, PROP_IDS['country'])
    names = [get_entity_name(qid) for qid in qids]
    return zip(qids, names)


def get_associated_country(entity_qid):
    countries = list(get_nationality(entity_qid))  # if the entity is a person
    if not countries:
        # if the entity is a location or a type that has a country associated with it
        countries = list(get_country(entity_qid))
    return countries


if __name__ == '__main__':
    qids = ['Q76', 'Q34221']
    for qid in qids:
        print(qid, get_entity_name(qid))
        print('Country:', get_associated_country(qid))
    