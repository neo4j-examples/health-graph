from py2neo import Graph, Node
import os

# ========================================== Node: Disclosure ==========================================#
def get_Disclosure_property (file):
    '''
    :param file: the xml file path to be parsed
    :return: a dictionary of properties of Disclsure
    {'pates', 'houseID', 'senateID', 'reportYear'}
    '''
    query = '''
    CALL apoc.load.xml({file})
    YIElD value
    WITH [attr in value._children
    WHERE attr._type in ['pages','houseID','senateID','reportYear'] | [attr._type, attr._text]] as pairs
    CALL apoc.map.fromPairs(pairs)
    YIELD value as properties
    RETURN properties
    '''

    return g.run(query, file = file).evaluate()


def create_Disclousure_node (properties, file):
    '''
    :param properties: a dictionary of properties of the node
    :param file: the xml file path to be parsed
    :return: node internal id
    '''
    query = '''
    CREATE(dc:Disclosure {properties})
    SET dc.fileid = {file}
    RETURN id(dc)
    '''

    index = '''
    CREATE INDEX ON: Disclosure(houseID)
    '''

    id = g.run(query, properties = properties, file = file[-13:-4]).evaluate()
    g.run(index)
    return id


# ========================================== Node: lobby firm ==========================================#
def get_LobbyFirm_property(file):
    '''
    :param file: the xml file path to be parsed
    :return: a dict of properties of LobbyFirm
    '''
    query = '''
        CALL apoc.load.xml({file})
        YIElD value
        WITH [attr in value._children
        WHERE attr._type in ['organizationName', 'firstName', 'lastName', 'address1',
        'address2', 'city', 'state', 'zip', 'country',
        'houseID'] | [attr._type, attr._text]] as pairs
        CALL apoc.map.fromPairs(pairs)
        YIELD value as properties
        RETURN properties
        '''
    pre_property = g.run(query, file=file).evaluate()
    property = {}
    # name
    # if pre_property['organizationName'] == None and pre_property['firstName'] == None and pre_property['lastName'] == None:
    #     property['name'] = 'NULL'

    if pre_property['organizationName']== None and pre_property['firstName'] != None and pre_property['lastName'] != None :
        property['name'] = str(pre_property['firstName'] + ' ' + pre_property['lastName'])

    elif pre_property['organizationName'] != None:
        property['name'] = pre_property['organizationName']

    #Address
    # if pre_property['address1']== None and pre_property['address2']== None:
    #     property['address'] = 'NULL'

    if pre_property['address1']!= None and pre_property['address2']!= None:
        property['address'] = str(pre_property['address1'] + ' ' + pre_property['address2'])

    elif pre_property['address1']!= None and pre_property['address2']== None:
        property['address'] = pre_property['address1']

    #city
    # if pre_property['city'] == None :
    #     property['city'] = 'NULL'

    if pre_property['city'] != None:
        property['city'] = pre_property['city']

    #State
    # if pre_property['state'] == None:
    #     property['state'] = 'NULL'

    if pre_property['state'] != None:
        property['state'] = pre_property['state']

    # Country
    if pre_property['country'] == None:
        property['country'] = 'USA'

    else:
        property['country'] = pre_property['country']

    # zip
    # if pre_property['zip'] == None:
    #     property['zip'] = 'NULL'

    if pre_property['zip'] != None:
        property['zip'] = pre_property['zip']

    # houseOrgId
    # if pre_property['houseID'] == None:
    #     property['houseOrgId'] = 'NULL'

    if pre_property['houseID'] != None:
        property['houseOrgId'] = pre_property['houseID'][:5]

    return property


def create_LobbyFirm_node(properties):
    '''
    :param properties: a dict of properties of the node
    :return: node internal id
    '''
    query = '''
        MERGE (lbf: LobbyFirm {houseOrgId:{houseOrgId}})
        ON CREATE SET lbf = {properties}
        RETURN id(lbf)
        '''

    index = '''
    CREATE INDEX ON: LobbyFirm(houseOrgId)
    '''
    id = g.run(query, houseOrgId = properties['houseOrgId'], properties=properties).evaluate()
    g.run(index)
    return id


# # ========================================== Node: clients ==========================================#
def get_Client_property(file):
    '''
    :param file: the xml file path to be parsed
    :return: a dict of properties of Client
    '''
    query = '''
        CALL apoc.load.xml({file})
        YIElD value
        WITH[attr in value._children WHERE
        attr._type in ['clientName'] | [attr._type, attr._text]] as pairs
        CALL apoc.map.fromPairs(pairs)
        YIELD value
        return value
        '''
    return g.run(query, file=file).evaluate()


def create_Client_node(properties):
    '''
    :param properties: the properties of the node
    :return: node internal id
    '''
    query = '''
    MERGE (cl:Client {clientName: {clientName}})
    RETURN id(cl) '''

    index = '''
    CREATE INDEX ON: Client(clientName)
    '''
    id = g.run(query, clientName = properties['clientName']).evaluate()
    g.run(index)
    return id


# #========================================== Node: Issue ==========================================#
def get_Issue_property(file):
    query = '''
    CALL apoc.load.xml({file})
    YIElD value UNWIND value._children as dc
    WITH dc WHERE dc._type = 'alis'
    UNWIND dc._children as alis
    UNWIND alis._children as ali_info
    WITH collect(ali_info) as issue
    RETURN issue
    '''
    pre_property = g.run(query, file = file).evaluate()
    properties = []
    dic = {}
    issueNumber = 0

    for i, property in enumerate(pre_property):
        if i%5 == 0:   #issueAreaCode
            dic = {}
            issueNumber += 1
            try:
                dic['issueAreaCode'] = property['_text']
            except KeyError:
                break
            dic['issueNumber'] = issueNumber
            properties.append(dic)

        elif (i - 1) % 5 == 0: #des
            try:
                dic['description'] = property['_children'][0]['_text']
            except KeyError:
                # dic['description'] = 'NULL'
                continue

        elif (i - 2) % 5 == 0: #fed_agen
            try:
                dic['federal_agencies'] = property['_text']
            except KeyError:
                # dic['federal_agencies'] = 'NULL'
                continue

    return properties


def create_Issue_node(properties):
    '''

    :param properties: a list of dict
    :return: the id of issue nodes
    '''

    id_lst = []
    query = '''
    CREATE (is: Issue {property})
    RETURN id(is)
    '''

    index = '''
    CREATE INDEX ON :Issue(issueNumber)
    '''
    for i, property in enumerate(properties):
        id = g.run(query, property = property).evaluate()
        # g.run(index)
        id_lst.append(id)
    g.run(index)
    return id_lst


# #========================================== Node: Lobbyist ==========================================#
def get_Lobbyist_property(file):
    query = '''
    CALL apoc.load.xml({file})
    YIElD value UNWIND value._children as dc
    WITH dc WHERE dc._type = 'alis'
    UNWIND dc._children as alis
    UNWIND alis._children as ali_info
    WITH collect(ali_info) as lobbyist
    RETURN lobbyist
    '''
    pre_property = g.run(query, file = file).evaluate()
    properties = []
    issueNumber = 1

    for i, property in enumerate(pre_property):

        if (i - 3) % 5 == 0: #lobbyists
            lobbyists = property['_children']
            for lobbyist in lobbyists:
                dic = {}
                if '_text' in (lobbyist['_children'][0]) and '_text' in (lobbyist['_children'][1]):
                    dic['issueNumber'] = issueNumber
                    dic['firstName'] = lobbyist['_children'][0]['_text']
                    dic['lastName'] = lobbyist['_children'][1]['_text']
                    try:
                        dic['position'] = lobbyist['_children'][3]['_text']
                    except KeyError:
                        dic['position'] = 'NULL'
                if dic:
                    properties.append(dic)
            issueNumber += 1
    properties = [d for d in properties if 'n/a' not in d.values()]
    return properties


def create_lobbyist_node(properties, issueID):
    '''

    :param properties: a list of dict
           issueID: a list of issue id
    :return: the id of issue nodes
    '''

    id_lst = []
    query = '''
    MERGE (lob: Lobbyist {firstName: {firstName}, lastName :{lastName}, position:{position}})
    WITH lob MATCH (is: Issue) WHERE id(is) = {issue_id}
    CREATE (lob)-[:LOBBIES]->(is)
    RETURN id(lob)
    '''

    index1 = '''
    CREATE INDEX ON :Lobbyist(firstName)
    '''

    index2 = '''
        CREATE INDEX ON :Lobbyist(lastName)
        '''

    index3 = '''
        CREATE INDEX ON :Lobbyist(position);
        '''

    for i, property in enumerate(properties):
        iss_index = property['issueNumber']-1
        issue_id = issueID[iss_index]
        id = g.run(query, firstName = property['firstName'], lastName = property['lastName'],
                   position = property['position'], issue_id = issue_id).evaluate()
        id_lst.append(id)

    g.run(index1)
    g.run(index2)
    g.run(index3)

    return id_lst


#========================================== Get files ==========================================#

if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)  ## readme need to document setting environment variable in pycharm
    g.delete_all()
    tx = g.begin()

    root =  os.getcwd()
    path = os.path.join(root, "data")
    disclosure_1st_path = os.path.join(path, "2013_1stQuarter_XML")
    files = [f for f in os.listdir(disclosure_1st_path) if f.endswith('.xml')]
    # files = ['file:///Users/yaqi/Documents/health-graph/data/2013_1stQuarter_XML/300529228.xml'] # Return xml files

    for file in files:
        fi = 'file://' + os.path.join(disclosure_1st_path, file)
        # fi = file
        print(fi)
        dc_pro = {}
        lbf_pro = {}
        cl_pro = {}
        is_pro = []
        lb_pro = []

        dc_id = ''
        lbf_id = ''
        cl_id = ''
        is_id = []
        lob_id = []

        dc_pro = get_Disclosure_property(fi)
        dc_id = create_Disclousure_node(dc_pro, fi)

        lbf_pro = get_LobbyFirm_property(fi)
        # print(lbf_pro)
        # print('type of properties:', type(lbf_pro))
        lbf_id = create_LobbyFirm_node(lbf_pro)

        cl_pro = get_Client_property(fi)
        cl_id = create_Client_node(cl_pro)

        is_pro = get_Issue_property(fi)

        if is_pro:
            is_id = create_Issue_node(is_pro)

            lb_pro = get_Lobbyist_property(fi)
            lob_id = create_lobbyist_node(lb_pro, is_id)


    # #========================================== Rel==========================================#
        lbf_dc_rel = g.run(
            '''MATCH (dc:Disclosure) WHERE id(dc) = {dc_id}
            MATCH (lbf:LobbyFirm) WHERE id(lbf) = {lbf_id}
            CREATE (lbf)-[r:FILED]->(dc)
            ''', dc_id = dc_id, lbf_id = lbf_id
        )

        cl_dc_rel = g.run(
            '''MATCH (dc:Disclosure) WHERE id(dc) = {dc_id}
            MATCH (cl:Client) WHERE id(cl) = {cl_id}
            CREATE (cl)-[r:SIGNED]->(dc)
            ''',
            dc_id= dc_id, cl_id =cl_id
        )

        if is_id:
            dc_iss_rel = g.run(
                ''' MATCH (dc:Disclosure) WHERE id(dc) = {dc_id}
                MATCH (is:Issue) WHERE id(is) in {is_id}
                CREATE (dc)-[r:HAS]->(is)
                ''', dc_id = dc_id, is_id = is_id
            )

        if lob_id:
            lob_lbf_rel = g.run(
                '''MATCH (lob:Lobbyist) WHERE id(lob) in {lob_id}
                MATCH (lbf:LobbyFirm) WHERE id(lbf) = {lbf_id}
                MERGE (lob)-[r:WORKS_AT]->(lbf)
                ''',
                lob_id = lob_id, lbf_id = lbf_id
            )




