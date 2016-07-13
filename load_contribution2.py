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


# # ========================================== Node: contribution ==========================================#
def get_contribution_property_cb(file):
    '''

    :param file:
    :return: a list of dict, each dict represent preperties for each contribution
    '''
    query = '''
    CALL apoc.load.xml({file}) YIElD value
    UNWIND value._children as CBS
    WITH CBS WHERE CBS._type ='contributions'
    UNWIND CBS._children as CB
    UNWIND CB._children as arr
    WITH collect(arr) as CBsInfo
    RETURN CBsInfo
    '''

    pre_property = g.run(query, file = file).evaluate()
    pre_property = [d for d in pre_property if '_text' in d]

    cb_num = 0
    property_lst = []
    dic = {}
    for i, info in enumerate(pre_property):

        if i % 6 == 0:
            dic = {}                               # type
            dic['type'] = info['_text']
            dic['con_num'] = cb_num
            cb_num += 1
            property_lst.append(dic)

        elif (i - 4) % 6 == 0:                     # amount
            dic['amount'] = info['_text']

        elif (i-5)%6  == 0:                          # date
            dic['date'] = info['_text']

    return property_lst


def create_contribution_node_cb(property_lst):
    '''

    :param property_lst:  a list of dict, each dict represent preperties for each contribution
    :return: a list of id
    '''
    query = '''
    CREATE (cb:Contribution {amount:{amount}, type:{tpe}, date:{date}})
    RETURN id(cb)
    '''

    index = '''
    CREATE INDEX ON: Contribution(type)
    '''

    id_lst = []
    for contribution in property_lst:

       id = g.run(query, amount = contribution['amount'], tpe = contribution['type'], date = contribution['date']).evaluate()
       id_lst.append(id)

    g.run(index)
    return id_lst


# # ========================================== Node: committee ==========================================#
def get_committee_property_cb(file):
    '''

    :param file:
    :return: a list of dict, each dict represent preperties for each contribution
    '''
    query = '''
    CALL apoc.load.xml({file}) YIElD value
    UNWIND value._children as CBS
    WITH CBS WHERE CBS._type ='contributions'
    UNWIND CBS._children as CB
    UNWIND CB._children as arr
    WITH collect(arr) as CBsInfo
    RETURN CBsInfo
    '''

    pre_property = g.run(query, file = file).evaluate()
    pre_property = [d for d in pre_property if '_text' in d]

    cb_num = 0
    property_lst = []
    for i, info in enumerate(pre_property):

        if (i - 2) % 6 == 0: # committee
            dic = {}
            dic['committee'] = info['_text']
            dic['con_num'] = cb_num
            cb_num += 1
            property_lst.append(dic)

    return property_lst


def create_committee_node(property_lst, contributionID):
    '''

    :param property_lst:  a list of dict, each dict represent preperties for each contribution
    :return: a list of id
    '''
    query = '''
    MERGE (com:Committee {name: {name}})
    WITH com
    MATCH(cb: Contribution) WHERE id(cb) = {contribution_id}
    CREATE(cb)-[:MADE_TO]->(com)
    RETURN id(com)
    '''

    index = '''
    CREATE INDEX ON: Committee(name)
    '''

    id_lst = []
    for committee in property_lst:
        idx = committee['con_num']
        contribution_id = contributionID[idx]
        id = g.run(query, name = committee['committee'], contribution_id = contribution_id).evaluate()
        id_lst.append(id)

    return id_lst


# # ========================================== Node: legislator ==========================================#
def get_legislator_property_cb(file):
    '''

    :param file:
    :return: a list of dict, each dict represent preperties for each contribution
    '''
    query = '''
    CALL apoc.load.xml({file}) YIElD value
    UNWIND value._children as CBS
    WITH CBS WHERE CBS._type ='contributions'
    UNWIND CBS._children as CB
    UNWIND CB._children as arr
    WITH collect(arr) as CBsInfo
    RETURN CBsInfo
    '''

    pre_property = g.run(query, file = file).evaluate()
    pre_property = [d for d in pre_property if '_text' in d]

    cb_num = 0
    property_lst = []
    for i, info in enumerate(pre_property):

        if (i - 3) % 6 == 0:
            dic = {} # legislator
            dic['legislator'] = info['_text']
            dic['con_num'] = cb_num
            cb_num += 1
            property_lst.append(dic)

    return property_lst


def create_legislator_node(property_lst, committeeID):
    '''

    :param property_lst:
    :param committeeID:
    :return:
    '''

    query = '''
    MERGE (ll:Legislator {name: {name}})
    WITH ll
    MATCH(com: Committee) WHERE id(com) = {committee_id}
    MERGE(com)-[:FUNDS]->(ll)
    RETURN id(ll)
    '''

    id_lst = []
    for legislator in property_lst:
        idx = legislator['con_num']
        committee_id = committeeID[idx]
        id = g.run(query, name = legislator['legislator'], committee_id = committee_id).evaluate()
        id_lst.append(id)

    return id_lst


# # ========================================== Node: contributerType ==========================================#
def contributerType(file):
    '''

    :param file:
    :return: a list of dict, each dict represent preperties for each contribution
    '''
    query = '''
    CALL apoc.load.xml({file}) YIElD value
    UNWIND value._children as CBS
    WITH CBS WHERE CBS._type ='contributions'
    UNWIND CBS._children as CB
    UNWIND CB._children as arr
    WITH collect(arr) as CBsInfo
    RETURN CBsInfo
    '''

    pre_property = g.run(query, file = file).evaluate()
    pre_property = [d for d in pre_property if '_text' in d]

    cb_num = 0
    property_lst = []
    for i, info in enumerate(pre_property):

        if (i - 1) % 6 == 0:  # contributorName
            dic = {}
            dic['contributorType'] = info['_text']
            dic['con_num'] = cb_num
            cb_num += 1
            property_lst.append(dic)

    return property_lst

def create_contributor_node(property, contribution_id ):
    query = '''
    MERGE (contributor: Contributor {name:{name}})
    WITH contributor
    MATCH(cb: Contribution) where id(cb) = {contribution_id}
    CREATE (contributor)-[:MADE]->(cb)
    RETURN id(contributor)
    '''
    return g.run(query, name = property['contributorType'], contribution_id = contribution_id).evaluate()


# #========================================== Get files ==========================================#

if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)  ## readme need to document setting environment variable in pycharm
    # g.delete_all()
    tx = g.begin()

    # root =  os.getcwd()
    # path = os.path.join(root, "data")
    # disclosure_1st_path = os.path.join(path, "2013_MidYear_XML")
    # files = [f for f in os.listdir(disclosure_1st_path) if f.endswith('.xml')]
    # files = ['file:///Users/yaqi/Documents/health-graph/data/2013_MidYear_XML/700669542.xml']  # Return xml files

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

    files = f1 + f2

    for file in files:
        # fi = 'file://' + os.path.join(disclosure_1st_path, file)
        fi = file
        print(fi)
        if has_contribution(fi):

            lf_pro = get_LobbyFirm_property_cb(fi)
            lf_id = create_LobbyFirm_node_cb(lf_pro, fi)

            cb_pro = get_contribution_property_cb(fi)
            cb_id = create_contribution_node_cb(cb_pro)

            com_pro = get_committee_property_cb(fi)
            com_id = create_committee_node(com_pro, cb_id)

            ll_pro = get_legislator_property_cb(fi)
            ll_id = create_legislator_node(ll_pro, com_id)

            cb_type = contributerType(fi)

            for contributor in cb_type:

                idx = contributor['con_num']
                contribution_id = cb_id[idx]

                if contributor['contributorType'] == 'Self':

                    if filer_type(fi) == 'L':

                        lb_pro = get_Lobbyist_property_cb(fi)
                        lb_id = create_Lobbyist_node_cb(lb_pro)

                        lob_lf_rel = g.run('''
                        MATCH (lob: Lobbyist) WHERE id(lob) = {lb_id}
                        MATCH (lf: LobbyFirm) WHERE id(lf) = {lf_id}
                        MERGE (lob)-[r:WORKS_AT]->(lf)
                        ''', lb_id = lb_id, lf_id = lf_id)

                        lb_cb_rel = g.run('''
                        MATCH (lob: Lobbyist) WHERE id(lob) = {lb_id}
                        MATCH (cb: Contribution) WHERE id(cb) = {contribution_id}
                        CREATE (lob)-[:FILED {self:1}]->(cb)
                        ''', lb_id = lb_id, contribution_id = contribution_id)

                    elif filer_type(fi) == 'O' :
                        lf_cb_rel = g.run('''
                            MATCH (lf: LobbyFirm) WHERE id(lf) = {lf_id}
                            MATCH (cb: Contribution) WHERE id(cb) = {contribution_id}
                            CREATE (lf)-[:FILED {self:1}]->(cb)
                            ''', lf_id=lf_id, contribution_id=contribution_id)

                else:

                    cbtor_id = create_contributor_node(contributor, contribution_id)

                    if filer_type(fi) == 'L':

                        lb_pro = get_Lobbyist_property_cb(fi)
                        lb_id = create_Lobbyist_node_cb(lb_pro)

                        lob_lf_rel = g.run('''
                                            MATCH (lob: Lobbyist) WHERE id(lob) = {lb_id}
                                            MATCH (lf: LobbyFirm) WHERE id(lf) = {lf_id}
                                            MERGE (lob)-[r:WORKS_AT]->(lf)
                                            ''', lb_id=lb_id, lf_id=lf_id)

                        lb_cb_rel = g.run('''
                                            MATCH (lob: Lobbyist) WHERE id(lob) = {lb_id}
                                            MATCH (cb: Contribution) WHERE id(cb) = {contribution_id}
                                            CREATE (lob)-[:FILED {self:0}]->(cb)
                                            ''', lb_id=lb_id, contribution_id=contribution_id)

                    elif filer_type(fi) == 'O':
                        lf_cb_rel = g.run('''
                                                MATCH (lf: LobbyFirm) WHERE id(lf) = {lf_id}
                                                MATCH (cb: Contribution) WHERE id(cb) = {contribution_id}
                                                CREATE (lf)-[:FILED {self:0}]->(cb)
                                                ''', lf_id=lf_id, contribution_id=contribution_id)










