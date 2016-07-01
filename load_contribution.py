from py2neo import Graph, Node
import os
from datetime import datetime

pw = os.environ.get('NEO4J_PASS')
g= Graph("http://localhost:7474/browser/",password = pw)  ## readme need to document setting environment variable in pycharm
# g.delete_all()
tx = g.begin()


#========================================== Get files ==========================================#
# root =  os.getcwd()
# path = os.path.join(root, "data")
# contribution_MidYear = os.path.join(path, "2013_MidYear_XML")
# files = [f for f in os.listdir(contribution_MidYear) if f.endswith('.xml')]
# files = ['file:///Users/yaqi/Documents/vir_health_graph/health-graph/data/2013_MidYear_XML/700669542.xml'] #  Return xml files


def get_file_path(kind):
    root_dir = '/Users/yaqi/Documents/data/' + kind
    filenames = [f for f in os.listdir(root_dir) if f.endswith('.xml')]
    filepath = []
    for file in filenames:
        path = 'file://' + os.path.join(root_dir, file)
        filepath.append(path)
    return filepath

f1 = get_file_path('2013_MidYear_XML')
f2 = get_file_path('2013_YearEnd_XML')

# print(len(f1))
# print(len(f2))
# print(len(f3))
# print(len(f4))
files = f1+f2

now_all = datetime.now()
for k, file in enumerate(files):
    now = datetime.now()
    # fi = 'file://' + os.path.join(contribution_MidYear, file)
    fi = file
    # print(fi)


# ========================================== Node: lobbyFirm, lobbyist ==========================================#
    lobbys_info = g.run(
        '''CALL apoc.load.xml({fi}) YIElD value
        WITH [attr in value._children WHERE attr._type in
        ['filerType','organizationName','lobbyistFirstName',
        'lobbyistLastName','senateRegID','houseRegID','noContributions']|[attr._type, attr._text]] as pairs
        CALL apoc.map.fromPairs(pairs)
        YIELD value
        RETURN value
        ''', parameters={'fi': fi}
    )

    for lobby_info in lobbys_info:
        # if contribution['value']['noContributions'] == 'true':
        filerType = lobby_info['value']['filerType']
        name = lobby_info['value']['organizationName']
        houseOrgId = lobby_info['value']['houseRegID']
        senateID = lobby_info['value']['senateRegID']
        firstName = lobby_info['value']['lobbyistFirstName']
        lastName = lobby_info['value']['lobbyistLastName']
        # print(contribution['value'])

    if filerType == 'L':

        lobbyist_node = g.run(
            ''' MERGE (lob: Lobbyist {firstName: {firstName}, lastName: {lastName}})
            RETURN id(lob)
            ''', parameters= {'firstName': firstName, 'lastName': lastName}
        )

        # lob_id = [lob['id(lob)'] for lob in lobbyist_node][0]
        lob_id = lobbyist_node.evaluate()

        lobbyFirm_node = g.run(
            ''' MERGE (lf: LobbyFirm {houseOrgId:{houseOrgId}})
            ON CREATE SET lf.name = {name}, lf.confilename = {fi}
            RETURN id(lf)
            ''', parameters= {'name': name, 'houseOrgId': houseOrgId, 'fi':fi[-14:]}
        )

        lf_id = lobbyFirm_node.evaluate()

        # print(lf_id, lob_id)

        lob_lf_rel = g.run(
            ''' MATCH (lob: Lobbyist) WHERE id(lob) = {lob_id}
            MATCH (lf: LobbyFirm) WHERE id(lf) = {lf_id}
            MERGE (lob)-[r:WORKS_AT]->(lf)
            ''', parameters={'lob_id': lob_id, 'lf_id':lf_id}
        )

    elif filerType == 'O':
        lobbyFirm_node = g.run(
            ''' MERGE (lf: LobbyFirm {houseOrgId:{houseOrgId}})
            ON CREATE SET lf.name = {name}
            RETURN id(lf)
            ''', parameters={'name': name, 'houseOrgId': houseOrgId}
        )

        lf_id = [lf['id(lf)'] for lf in lobbyFirm_node][0]


# ========================================== Node: contribution ==========================================#
    contributions_info = g.run(
        '''CALL apoc.load.xml({fi}) YIElD value
        UNWIND value._children as CB
        WITH CB WHERE CB._type in ['noContributions', 'contributions']
        RETURN CB
        ''', parameters={'fi': fi}
    )

    for contribution_info in contributions_info:

        if contribution_info['CB']['_type'] == 'noContributions':
            noContributions = contribution_info['CB']['_text']

            if noContributions == 'true':
                break

        elif contribution_info['CB']['_type'] == 'contributions':
            contributions = contribution_info['CB']['_children']

            for contribution_list in contributions:
                contribution_key = []
                contribution_value = []
                for contribution_dic in contribution_list['_children']:
                    if '_text' in contribution_dic:
                        contribution_key.append(contribution_dic['_type'])
                        contribution_value.append(contribution_dic['_text'])

                if contribution_key and contribution_value:
                    dic = dict(zip(contribution_key, contribution_value))
                    date = dic['date']
                    contributorName = dic['contributorName']
                    tpe = dic['type']
                    legislator = dic['recipientName']
                    committee = dic['payeeName']
                    amount = dic['amount']

                    contribution_node = g.run(
                        '''CREATE (cb:Contribution {amount:{amount}, type:{tpe}})
                        RETURN id(cb)
                        ''', parameters={'amount': amount, 'tpe': tpe}
                    )

                    cb_id = contribution_node.evaluate()

                    legislator_node = g.run(
                        '''CREATE (ll:Legislator {name: {legislator}})
                        RETURN id(ll)
                        ''', parameters={'legislator': legislator}
                    )
                    ll_id = legislator_node.evaluate()

                    committee_node = g.run(
                        '''CREATE (com:Committee {name: {committee}})
                        RETURN id(com)
                        ''', parameters={'committee': committee}
                    )
                    com_id = committee_node.evaluate()

                    com_ll_rel = g.run(
                        '''MATCH (com:Committee) WHERE id(com) = {com_id}
                        MATCH (ll: Legislator) WHERE id(ll) = {ll_id}
                        CREATE (com)-[r:FUNDS]->(ll)
                        ''', parameters= {'com_id': com_id, 'll_id':ll_id}
                    )

                    com_cb_rel = g.run(
                        '''MATCH (com:Committee) WHERE id(com) = {com_id}
                        MATCH (cb: Contribution) WHERE id(cb) = {cb_id}
                        CREATE (cb)-[r:MADE_TO]->(com)
                        ''', parameters={'com_id':com_id, 'cb_id':cb_id}
                    )


                    if contributorName == 'Self' and filerType == 'L':
                        lob_cb_rel = g.run(
                            ''' MATCH (cb: Contribution) WHERE id(cb) = {cb_id}
                            MATCH (lob: Lobbyist) WHERE id(lob) = {lob_id}
                            CREATE (lob)-[r: MADE {date: {date}}]-> (cb)
                            ''', parameters={'date': date, 'cb_id': cb_id, 'lob_id': lob_id }
                        )
                    elif contributorName == 'Self' and filerType == 'O':
                        lf_cb_rel = g.run(
                            ''' MATCH (cb: Contribution) WHERE id(cb) = {cb_id}
                            MATCH (lf: LobbyFirm) WHERE id(lf) = {lf_id}
                            CREATE (lf)-[r: MADE {date: {date}}]-> (cb)
                            ''', parameters={'date': date, 'cb_id': cb_id, 'lf_id': lf_id}
                        )

                    elif contributorName != 'Self':
                        print(fi)
    end = datetime.now()
    print(k)
    print(end-now)

end_all = datetime.now()
print (end_all - now_all)

