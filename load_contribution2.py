from py2neo import Graph, Node
import os
from datetime import datetime
# ========================================== check contribution and filer type ==========================================#
def filer_type(file):
    '''

    :param file:
    :return: a string. 'L' represent lobbyist, 'O' represent Lobby Firm
    '''

    query = '''
    CALL apoc.load.xml({file}) YIElD value
    UNWIND value._children as result
    WITH result WHERE result._type = 'filerType'
    RETURN result._text
    '''

    return g.run(query, file = file).evaluate()


def has_contribution(file):
    '''

    :param file:
    :return: boolean, f no contribution, t has contribution
    '''

    query = '''
    CALL apoc.load.xml({file}) YIElD value
    UNWIND value._children as result
    WITH result WHERE result._type = 'noContributions'
    RETURN result
    '''

    result = g.run(query, file = file).evaluate()

    try:
        noContribution = result['_text']
    except KeyError:
        noContribution = 'false'

    return noContribution in ['F', "FALSE", 'false', 'NO', 'no', 'No']


# ========================================== Node: lobbyFirm ==========================================#
def get_LobbyFirm_property_cb (file):
    '''
    :param file:
    :return: a dictionary of lobbyfirm properties
    '''

    query = '''
    CALL apoc.load.xml({file}) YIElD value
        WITH [attr in value._children WHERE attr._type in
        ['organizationName', 'houseRegID']|[attr._type, attr._text]] as pairs
        CALL apoc.map.fromPairs(pairs)
        YIELD value
        RETURN value
    '''

    return g.run(query, file = file).evaluate()


def create_LobbyFirm_node_cb(properties, file):
    '''

    :param properties: a dictionary of lobbyfirm properties
    :return: an int, the internal node id
    '''
    query = '''
    MERGE (lf: LobbyFirm {houseOrgId:{houseOrgId}})
    ON CREATE SET lf.organizationName = {organizationName}, lf.confileId = {file}
    RETURN id(lf)
    '''

    return g.run(query, organizationName = properties['organizationName'] ,
                 houseOrgId = properties['houseRegID'], file=file[-13:-4]).evaluate()

# ========================================== Node: lobbyist ==========================================#
def get_Lobbyist_property_cb (file):
    '''
    :param file:
    :return: a dictionary of lobbyist properties
    '''

    query = '''
    CALL apoc.load.xml({file}) YIElD value
        WITH [attr in value._children WHERE attr._type in
        ['lobbyistFirstName', 'lobbyistLastName']|[attr._type, attr._text]] as pairs
        CALL apoc.map.fromPairs(pairs)
        YIELD value
        RETURN value
    '''

    return g.run(query, file = file).evaluate()


def create_Lobbyist_node_cb(properties):
    '''

    :param properties: a dictionary of lobbyfirm properties
    :return: an int, the internal node id
    '''
    query = '''
    MERGE (lob: Lobbyist {firstName: {firstName}, lastName: {lastName}})
    RETURN id(lob)
    '''

    return g.run(query, firstName = properties['lobbyistFirstName'] ,
                 lastName = properties['lobbyistLastName']).evaluate()


#
#
#     lobbys_info = g.run(
#         '''CALL apoc.load.xml({fi}) YIElD value
#         WITH [attr in value._children WHERE attr._type in
#         ['filerType','organizationName','lobbyistFirstName',
#         'lobbyistLastName','senateRegID','houseRegID','noContributions']|[attr._type, attr._text]] as pairs
#         CALL apoc.map.fromPairs(pairs)
#         YIELD value
#         RETURN value
#         ''', parameters={'fi': fi}
#     )
#
#     for lobby_info in lobbys_info:
#         # if contribution['value']['noContributions'] == 'true':
#         filerType = lobby_info['value']['filerType']
#         name = lobby_info['value']['organizationName']
#         houseOrgId = lobby_info['value']['houseRegID']
#         senateID = lobby_info['value']['senateRegID']
#         firstName = lobby_info['value']['lobbyistFirstName']
#         lastName = lobby_info['value']['lobbyistLastName']
#         # print(contribution['value'])
#
#     if filerType == 'L':
#
#         lobbyist_node = g.run(
#             ''' MERGE (lob: Lobbyist {firstName: {firstName}, lastName: {lastName}})
#             RETURN id(lob)
#             ''', parameters= {'firstName': firstName, 'lastName': lastName}
#         )
#
#         # lob_id = [lob['id(lob)'] for lob in lobbyist_node][0]
#         lob_id = lobbyist_node.evaluate()
#
#         lobbyFirm_node = g.run(
#             ''' MERGE (lf: LobbyFirm {houseOrgId:{houseOrgId}})
#             ON CREATE SET lf.name = {name}, lf.confilename = {fi}
#             RETURN id(lf)
#             ''', parameters= {'name': name, 'houseOrgId': houseOrgId, 'fi':fi[-14:]}
#         )
#
#         lf_id = lobbyFirm_node.evaluate()
#
#         # print(lf_id, lob_id)
#
#         lob_lf_rel = g.run(
#             ''' MATCH (lob: Lobbyist) WHERE id(lob) = {lob_id}
#             MATCH (lf: LobbyFirm) WHERE id(lf) = {lf_id}
#             MERGE (lob)-[r:WORKS_AT]->(lf)
#             ''', parameters={'lob_id': lob_id, 'lf_id':lf_id}
#         )
#
#     elif filerType == 'O':
#         lobbyFirm_node = g.run(
#             ''' MERGE (lf: LobbyFirm {houseOrgId:{houseOrgId}})
#             ON CREATE SET lf.name = {name}
#             RETURN id(lf)
#             ''', parameters={'name': name, 'houseOrgId': houseOrgId}
#         )
#
#         lf_id = [lf['id(lf)'] for lf in lobbyFirm_node][0]
#
#
# # ========================================== Node: contribution ==========================================#
def get_contribution_property_cb(file):
    pass

#     contributions_info = g.run(
#         '''CALL apoc.load.xml({fi}) YIElD value
#         UNWIND value._children as CB
#         WITH CB WHERE CB._type in ['noContributions', 'contributions']
#         RETURN CB
#         ''', parameters={'fi': fi}
#     )
#
#     for contribution_info in contributions_info:
#
#         if contribution_info['CB']['_type'] == 'noContributions':
#             try:
#                 noContributions = contribution_info['CB']['_text']
#             except KeyError:
#                 noContributions = 'false'
#
#             if noContributions == 'true':
#                 break
#
#         elif contribution_info['CB']['_type'] == 'contributions':
#             contributions = contribution_info['CB']['_children']
#
#             for contribution_list in contributions:
#                 contribution_key = []
#                 contribution_value = []
#                 for contribution_dic in contribution_list['_children']:
#                     if '_text' in contribution_dic:
#                         contribution_key.append(contribution_dic['_type'])
#                         contribution_value.append(contribution_dic['_text'])
#
#                 if contribution_key and contribution_value:
#                     dic = dict(zip(contribution_key, contribution_value))
#                     date = dic['date']
#                     contributorName = dic['contributorName']
#                     tpe = dic['type']
#                     legislator = dic['recipientName']
#                     committee = dic['payeeName']
#                     amount = dic['amount']
#
#                     contribution_node = g.run(
#                         '''CREATE (cb:Contribution {amount:{amount}, type:{tpe}})
#                         RETURN id(cb)
#                         ''', parameters={'amount': amount, 'tpe': tpe}
#                     )
#
#                     cb_id = contribution_node.evaluate()
#
#                     legislator_node = g.run(
#                         '''CREATE (ll:Legislator {name: {legislator}})
#                         RETURN id(ll)
#                         ''', parameters={'legislator': legislator}
#                     )
#                     ll_id = legislator_node.evaluate()
#
#                     committee_node = g.run(
#                         '''CREATE (com:Committee {name: {committee}})
#                         RETURN id(com)
#                         ''', parameters={'committee': committee}
#                     )
#                     com_id = committee_node.evaluate()
#
#                     com_ll_rel = g.run(
#                         '''MATCH (com:Committee) WHERE id(com) = {com_id}
#                         MATCH (ll: Legislator) WHERE id(ll) = {ll_id}
#                         CREATE (com)-[r:FUNDS]->(ll)
#                         ''', parameters= {'com_id': com_id, 'll_id':ll_id}
#                     )
#
#                     com_cb_rel = g.run(
#                         '''MATCH (com:Committee) WHERE id(com) = {com_id}
#                         MATCH (cb: Contribution) WHERE id(cb) = {cb_id}
#                         CREATE (cb)-[r:MADE_TO]->(com)
#                         ''', parameters={'com_id':com_id, 'cb_id':cb_id}
#                     )
#
#
#                     if contributorName == 'Self' and filerType == 'L':
#                         lob_cb_rel = g.run(
#                             ''' MATCH (cb: Contribution) WHERE id(cb) = {cb_id}
#                             MATCH (lob: Lobbyist) WHERE id(lob) = {lob_id}
#                             CREATE (lob)-[r: MADE {date: {date}}]-> (cb)
#                             ''', parameters={'date': date, 'cb_id': cb_id, 'lob_id': lob_id }
#                         )
#                     elif contributorName == 'Self' and filerType == 'O':
#                         lf_cb_rel = g.run(
#                             ''' MATCH (cb: Contribution) WHERE id(cb) = {cb_id}
#                             MATCH (lf: LobbyFirm) WHERE id(lf) = {lf_id}
#                             CREATE (lf)-[r: MADE {date: {date}}]-> (cb)
#                             ''', parameters={'date': date, 'cb_id': cb_id, 'lf_id': lf_id}
#                         )
#
#                     elif contributorName != 'Self':
#                         print(fi)
#     end = datetime.now()
#     print(k)
#     print(end-now)
#
# end_all = datetime.now()
# print (end_all - now_all)
# #========================================== Get files ==========================================#

if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)  ## readme need to document setting environment variable in pycharm
    g.delete_all()
    tx = g.begin()

    root =  os.getcwd()
    path = os.path.join(root, "data")
    disclosure_1st_path = os.path.join(path, "2013_MidYear_XML")
    # files = [f for f in os.listdir(disclosure_1st_path) if f.endswith('.xml')]
    files = ['file:///Users/yaqi/Documents/health-graph/data/2013_MidYear_XML/700669542.xml']  # Return xml files

    for file in files:
        # fi = 'file://' + os.path.join(disclosure_1st_path, file)
        fi = file
        print(fi)

        filertype = filer_type(fi)
        print(filertype)
        contribution = has_contribution(fi)
        print(contribution)

        lf_pro = get_LobbyFirm_property_cb(fi)
        print(lf_pro)

        lf_id = create_LobbyFirm_node_cb(lf_pro, fi)
        print(type(lf_id))

        lb_pro = get_Lobbyist_property_cb(fi)
        print(lb_pro)

        lb_id = create_Lobbyist_node_cb(lb_pro)
        print(lb_id)
