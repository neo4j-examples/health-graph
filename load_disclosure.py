from py2neo import Graph, Node
import os

# ========================================== Node: Disclosure ==========================================#

def get_Disclosure_property (file):
    '''
    :param file: the xml file path to be parsed
    :return: properties of Disclosure
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
    :param properties: the properties of the node
    :return: node internal id
    '''
    query = '''
    CREATE(dc:Disclosure {properties})
    SET dc.fileid = {file}
    RETURN id(dc)
    '''

    return g.run(query, properties = properties, file = file)


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
    if pre_property['organizationName'] == None and pre_property['firstName'] == None and pre_property['lastName'] == None:
        property['name'] = None

    elif pre_property['organizationName']== None and pre_property['firstName'] != None and pre_property['lastName'] != None :
        property['name'] = str(pre_property['firstName'] + ' ' + pre_property['lastName'])

    elif pre_property['organizationName'] != None:
        property['name'] = pre_property['organizationName']

    #Address
    if pre_property['address1']== None and pre_property['address2']== None:
        property['address'] = None

    elif pre_property['address1']!= None and pre_property['address2']!= None:
        property['address'] = str(pre_property['address1'] + ' ' + pre_property['address2'])

    elif pre_property['address1']!= None and pre_property['address2']== None:
        property['address'] = pre_property['address1']

    #city
    if pre_property['city'] == None :
        property['city'] = None

    else:
        property['city'] = pre_property['city']

    #State
    if pre_property['state'] == None:
        property['state'] = None

    else:
        property['state'] = pre_property['state']

    # Country
    if pre_property['country'] == None:
        property['country'] = 'USA'

    else:
        property['country'] = pre_property['country']

    # zip
    if pre_property['zip'] == None:
        property['zip'] = None

    else:
        property['zip'] = pre_property['zip']

    # houseOrgId
    if pre_property['houseID'] == None:
        property['houseOrgId'] = None

    else:
        property['houseOrgId'] = pre_property['houseID'][:5]

    return property


def create_LobbyFirm_node(properties):
    '''
    :param properties: the properties of the node
    :return: node internal id
    '''
    query = '''
        MERGE (lf: LobbyFirm {houseOrgId:{houseOrgId}})
        ON CREATE SET lf = {properties}
        RETURN id(lf)
        '''

    return g.run(query, houseOrgId = properties['houseOrgId'], properties=properties)


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

    return g.run(query, clientName = properties['clientName'])


# #========================================== Node: Issue ==========================================#
def create_Issue_property(file):
    query = '''
    CALL apoc.load.xml({file})
    YIElD value UNWIND value._children as dc
    WITH dc WHERE dc._type = 'alis'
    UNWIND dc._children as alis
    UNWIND alis._children as ali_info
    WITH collect(ali_info) as arr
    RETURN arr
    '''
    pre_property = g.run(query, file = file).evaluate()
    properties = []
    dic = {}
    issueNumber = 0

    for i, property in enumerate(pre_property):
        if i%5 == 0:   #issueAreaCode
            dic = {}
            issueNumber += 1
            dic['issueNumber'] = issueNumber
            dic['issueAreaCode'] = property['_text']
            properties.append(dic)

        elif (i - 1) % 5 == 0: #des
            dic['description'] = property['_children'][0]['_text']

        elif (i - 2) % 5 == 0: #fed_agen
            dic['federal_agencies'] = property['_text']

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
    for i, property in enumerate(properties):
        id = g.run(query, property = property).evaluate()
        id_lst.append(id)

    return id_lst

# #========================================== Node: Lobbyyist ==========================================#

def create_Issue_property(file):
    query = '''
    CALL apoc.load.xml({file})
    YIElD value UNWIND value._children as dc
    WITH dc WHERE dc._type = 'alis'
    UNWIND dc._children as alis
    UNWIND alis._children as ali_info
    WITH collect(ali_info) as arr
    RETURN arr
    '''
    pre_property = g.run(query, file = file).evaluate()
    properties = []
    dic = {}
    issueNumber = 0

    for i, property in enumerate(pre_property):
        if i%5 == 0:   #issueAreaCode
            dic = {}
            issueNumber += 1
            dic['issueNumber'] = issueNumber
            dic['issueAreaCode'] = property['_text']
            properties.append(dic)

        elif (i - 1) % 5 == 0: #des
            dic['description'] = property['_children'][0]['_text']

        elif (i - 2) % 5 == 0: #fed_agen
            dic['federal_agencies'] = property['_text']

    return properties


#     # Cypher returns a cursor object, each item in the cursor object represents a issuecode and lobbyists who are associated to that issuecode
#
#     iscd_lst = []
#     # collect issuecodes of one disclosure
#     lobId_collector = []
#
#     # find_empty = False
#     #find_empty data
#     for i,lobbyist_issuecode in enumerate(lobbyists_issuecode):
#
#         lobOrIss_item = lobbyist_issuecode['ali_info']
#         #each item in the cursor object is a JSON object and represents a issuecode and lobbyists who are associated to that issuecode
#         # Both lobbyists and issuecodes in the JSON object have attr '_type';
#         # Lobbyists have attr '_children'
#         # Issuecodes have attr '_text'
#         if not('_text' in lobOrIss_item or '_children' in lobOrIss_item ):
#             # The if statement check if an item doest not have attr '_text' nor '_children', the JSON object has no
#             # issuecode data or lobbyists data to be extracted
#             find_empty = True
#             # here find empty data
#             break
#
#         elif '_text' in lobOrIss_item:
#             # check if '_text' in the item, the item represents an issuecode
#             issuecode = lobOrIss_item['_text']
#             iscd_lst.append(issuecode)
#             # collecting issuecodes to the idscd_lst
#             issues_node = g.run(
#                 '''CREATE (iss:Issue {code: {code}})
#                 RETURN id(iss)
#                 ''', parameters = {'code': issuecode}
#             )
#             issue_id = [issue['id(iss)'] for issue in issues_node][0]
#             issue_id_lst.append(issue_id)
#             # Collect node'id of new (iss: Issue) created at each iteration
#
#         elif '_children' in lobOrIss_item :
#             # check if '_children' in the item, the item represents lobbyists
#             #lobbyists_json = lobOrIss_item
#             lobbyists = lobOrIss_item['_children']
#             lob_id_lst = []
#
#             for lobbyist in lobbyists:
#
#                 lobbyist_dic = {}
#                 first_name = lobbyist['_children'][0]
#                 last_name = lobbyist['_children'][1]
#                 position = lobbyist['_children'][3]
#
#                 if ('_text' in first_name) and ('_text' in last_name):
#
#                     lobbyist_dic['firstName'] = first_name['_text']
#                     lobbyist_dic['lastName'] = last_name['_text']
#
#                     try:
#
#                         lobbyist_dic['position'] = position['_text']
#                         # position is optional in the XML file
#
#                     except KeyError:
#
#                         lobbyist_dic['position'] = "NULL"
#
#                     # lobbyist_lst.append(lobbyist_dic)
#                     lobbyists_node = g.run(   # Use MERGE vs CREATE: MERGE can be problematic if 2 persons have same name and position
#                         ''' MERGE (lob: Lobbyist {firstName: {firstName}, lastName: {lastName}, position : {position}})
#                         RETURN id(lob)
#                         ''', parameters={'firstName': lobbyist_dic['firstName'],
#                                          'lastName': lobbyist_dic['lastName'],
#                                          'position': lobbyist_dic['position']}
#                     )
#                     lob_id = [lobbyist['id(lob)'] for lobbyist in lobbyists_node][0]
#                     lob_id_lst.append(lob_id)
#                     lobId_collector.append(lob_id)
#
#             # print(issue_id)
#             # print(lobbyist_lst)
#             # print(lob_id_lst)
#             lobbyist_iss_rel = g.run(
#                 '''Match (iss:Issue) WHERE id(iss) = {issue_id}
#                 MATCH (lob: Lobbyist) WHERE id(lob) in {lob_id_lst}
#                 CREATE (lob)-[r:LOBBIES]->(iss)
#                 ''', parameters={'issue_id': issue_id, 'lob_id_lst': lob_id_lst}
#             )
#
#
#     print(lobId_collector)
#     # if find_empty:
#     #     break
# # ========================================== Node: Issue ==========================================#
#     issuesdes = g.run(
#         '''CALL apoc.load.xml({f})
#         YIElD value UNWIND value._children as dl
#         WITH dl WHERE dl._type = 'alis'
#         UNWIND dl._children as alis
#         UNWIND alis._children as ali_info
#         WITH ali_info WHERE ali_info._type = 'specific_issues'
#         UNWIND ali_info._children as specific_issues
#         RETURN specific_issues._text''', parameters={'f': fi})
#
#     # desc = [des['specific_issues._text'] for des in issuesdes][0]
#     # print(desc) "None"
#
#
#     federal_agencies = g.run(
#         '''CALL apoc.load.xml({f})
#         YIElD value UNWIND value._children as dl
#         WITH dl WHERE dl._type = 'alis'
#         UNWIND dl._children as alis
#         UNWIND alis._children as ali_info
#         WITH ali_info WHERE ali_info._type = 'federal_agencies'
#         RETURN ali_info._text''', parameters={'f': fi})
#
#     isde_lst = []
#     feag_lst = []
#
#     for issuedes in issuesdes:
#         if issuedes['specific_issues._text'] != None :
#             isde_lst.append(issuedes['specific_issues._text'])
#
#     for agency in federal_agencies:
#         if agency['ali_info._text'] != None:
#             feag_lst.append(agency['ali_info._text'])
#     # print(fi)
#     # print(len(iscd_lst))
#     # print(len(isde_lst))
#     # print(len(feag_lst))
#
#     if len(iscd_lst) == len(isde_lst) and len(isde_lst) == len(feag_lst):
#
#         for i in range(len(iscd_lst)):
#             issues = g.run(
#                 ''' MATCH (iss:Issue) WHERE id(iss) in {issue_id_lst}
#                 SET iss.des = {des}, iss.agency = {agency}
#                 ''', parameters={'issue_id_lst': issue_id_lst, "des" : isde_lst[i], "agency": feag_lst[i]}
#             )
#     else:
#         print('error')
#
#
# #========================================== Rel==========================================#
#     dc_iss_rel = g.run(
#         '''MATCH (dc:Disclosure) WHERE id(dc) = {dc_id}
#         MATCH (iss:Issue) WHERE id(iss) in {iss_id}
#         CREATE (dc)-[r:HAS]->(iss)
#         ''',
#         parameters = { 'dc_id': dc_id, 'iss_id': issue_id_lst}
#     )
#
#     lf_dc_rel = g.run(
#         '''MATCH (dc:Disclosure) WHERE id(dc) = {dc_id}
#         MATCH (lf:LobbyFirm) WHERE id(lf) = {lf_id}
#         CREATE (lf)-[r:FILED]->(dc)
#         ''',
#         parameters={'dc_id': dc_id, 'lf_id': lf_id}
#     )
#
#     lob_lf_rel = g.run(
#         '''MATCH (lob:Lobbyist) WHERE id(lob) in {lobId_collector}
#         MATCH (lf:LobbyFirm) WHERE id(lf) = {lf_id}
#         MERGE (lob)-[r:WORKS_AT]->(lf)
#         ''',
#         parameters={'lobId_collector': lobId_collector, 'lf_id': lf_id}
#     )
#
#     cl_dc_rel = g.run(
#         '''MATCH (dc:Disclosure) WHERE id(dc) = {dc_id}
#         MATCH (cl:Client) WHERE id(cl) = {cl_id}
#         CREATE (cl)-[r:SIGNED]->(dc)
#         ''',
#         parameters={'dc_id': dc_id, 'cl_id': cl_id}
#     )


pw = os.environ.get('NEO4J_PASS')
# g= Graph("http://localhost:7474/browser/",password = pw)
g= Graph("http://localhost:7474/",password = pw)## readme need to document setting environment variable in pycharm
g.delete_all()
tx = g.begin()


#========================================== Get files ==========================================#
root =  os.getcwd()
path = os.path.join(root, "data")
disclosure_1st_path = os.path.join(path, "2013_1stQuarter_XML")
# files = [f for f in os.listdir(disclosure_1st_path) if f.endswith('.xml')]
files = ['file:///Users/yaqi/Documents/vir_health_graph/health-graph/data/2013_1stQuarter_XML/300529246.xml'] # Return xml files
for file in files:
    # fi = 'file://' + os.path.join(disclosure_1st_path, file)
    fi = file
    print(fi)

    dc_pro = get_Disclosure_property(fi)
    dc_nodes = create_Disclousure_node(dc_pro, fi)

    lf_pro = get_LobbyFirm_property(fi)
    lf_nodes = create_LobbyFirm_node(lf_pro)

    cl_pro = get_Client_property(fi)
    cl_nodes = create_Client_node(cl_pro)

    is_pro = create_Issue_property(fi)
    is_nodes = create_Issue_node(is_pro)
    print(is_nodes)
