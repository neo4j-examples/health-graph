import requests

def get_Rxcui_fromNDC(data):
    url_getRxId = "https://rxnav.nlm.nih.gov/REST/ndcproperties.json?"
    resource = {'id': data}
    try:
        Rxcui = requests.get(url_getRxId, params=resource).json()['ndcPropertyList']['ndcProperty'][0]['rxcui']
    except (TypeError, KeyError):
        Rxcui = ''

    return Rxcui


def get_Rxcui_fromName(data):
    url_getRxId = 'https://rxnav.nlm.nih.gov/REST/rxcui.json?'
    resource = {'name': data, 'allsrc': 1, 'search': 2}
    try:
        Rxcui = requests.get(url_getRxId, params=resource).json()["idGroup"]['rxnormId'][0]
    except (TypeError, KeyError):
        Rxcui = ''

    return Rxcui



