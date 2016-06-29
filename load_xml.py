from py2neo import Graph, Node
import os


pw = os.environ.get('NEO4J_PASS')
g= Graph("http://localhost:7474/browser/",password = pw)  ## readme need to document setting environment variable in pycharm
g.delete_all()
tx = g.begin()


#========================================== Get files ==========================================#
root =  os.getcwd()
path = os.path.join(root, "data")
disclosure_1st_path = os.path.join(path, "2013_1stQuarter_XML")
files = [f for f in os.listdir(disclosure_1st_path) if f.endswith('.xml')]
# files = ['file:///Users/yaqi/Documents/vir_health_graph/health-graph/data/2013_1stQuarter_XML/300545488.xml'] # Return xml files
for file in files:
    fi = 'file://' + os.path.join(disclosure_1st_path, file)
    # fi = file
    print(fi)


# ========================================== Node: Disclosure ==========================================#
    dc_id = ''
    # dc_id is set to '' at the beginning of each iteration
    disclosure = g.run(
        '''CALL apoc.load.xml({f})
        YIElD value as ld2
        WITH [attr in ld2._children
        WHERE attr._type in ['pages','houseID','senateID','reportYear'] | [attr._type, attr._text]] as pairs
        CALL apoc.map.fromPairs(pairs)
        YIELD value
        CREATE (dc:Disclosure {pages: value.pages, senateID: value.senateID,
        houseID: value.houseID, reportYear: value.reportYear, fileid : {fi}})
        RETURN id(dc)''',
        parameters={'f': fi, 'fi':fi[-14:]}
    )
    dc_id = [dc['id(dc)'] for dc in disclosure][0]
    # disclosure is a cursor object, dc ['id(dc)'] return a list.


# ========================================== Node: lobby firm ==========================================#
    lf_id = ''
    lobbyFirms = g.run(
        '''CALL apoc.load.xml({f})
        YIElD value as lf
        WITH[attr in lf._children WHERE
        attr._type in ['organizationName', 'firstName', 'lastName', 'address1','address2', 'city', 'state', 'zip', 'country',
        'houseID'] | [attr._type, attr._text]] as pairs
        CALL apoc.map.fromPairs(pairs)
        YIELD value
        return value''',
        parameters = {'f': fi}
    )

    for lobbyFirm in lobbyFirms:
        # Name property
        if lobbyFirm['value']['organizationName']== None and lobbyFirm['value']['firstName'] == None and lobbyFirm['value']['lastName'] == None :
            name = None
        elif lobbyFirm['value']['organizationName']== None and lobbyFirm['value']['firstName'] != None and lobbyFirm['value']['lastName'] != None :
            name = str(lobbyFirm['value']['firstName'] + ' ' + lobbyFirm['value']['lastName'])
        elif lobbyFirm['value']['organizationName'] != None:
            name = lobbyFirm['value']['organizationName']

        #Address
        if lobbyFirm['value']['address1']== None and lobbyFirm['value']['address2']== None:
            address = None
        elif lobbyFirm['value']['address1']!= None and lobbyFirm['value']['address2']!= None:
            address = str(lobbyFirm['value']['address1'] + ' ' + lobbyFirm['value']['address2'])
        elif lobbyFirm['value']['address1']!= None and lobbyFirm['value']['address2']== None:
            address = lobbyFirm['value']['address1']
        #city
        if lobbyFirm['value']['city'] == None :
            city = None
        else:
            city = lobbyFirm['value']['city']

        #State
        if lobbyFirm['value']['state'] == None:
            state = None
        else:
            state = lobbyFirm['value']['state']

        # Country
        if lobbyFirm['value']['country'] == None:
            country = 'USA'
        else:
            country = lobbyFirm['value']['country']

        # zip
        if lobbyFirm['value']['zip'] == None:
            zip = None
        else:
            zip = lobbyFirm['value']['zip']

        # houseOrgId
        if lobbyFirm['value']['houseID'] == None:
            houseOrgId = None
        else:
            houseOrgId = lobbyFirm['value']['houseID'][:5]

    lobbyFirm_node = g.run('''
    MERGE (lf: LobbyFirm {houseOrgId:{houseOrgId}})
    ON CREATE SET lf.name = {name}, lf.address = {address}, lf.city = {city},
    lf.state = {state}, lf.country = {country}, lf.zip = {zip}
    RETURN id(lf)''', parameters= {'name': name, 'address': address, 'city': city, 'state':state, 'country': country,
                                   'zip': zip, 'houseOrgId': houseOrgId}
                           )

    lf_id = [lf['id(lf)'] for lf in lobbyFirm_node][0]



# ========================================== Node: clients ==========================================#
    # TODO(Yaqi): add code here
    cl_id = ''
    clients = g.run(
        '''CALL apoc.load.xml({f})
        YIElD value as cl
        WITH[attr in cl._children WHERE
        attr._type in ['clientName'] | [attr._type, attr._text]] as pairs
        CALL apoc.map.fromPairs(pairs)
        YIELD value
        return value''',
        parameters={'f': fi}
    )
    for client in clients:
        client_name = client['value']['clientName']

        if client_name:
            client_node = g.run(
                '''MERGE (cl:Client {name: {client_name}})
                RETURN id(cl)
                ''', parameters= {'client_name': client_name}
            )

    cl_id = [cl['id(cl)'] for cl in client_node][0]

#========================================== Node: lobbyist ==========================================#
    issue_id_lst = []
    # issue_id_lst is set to [] at the beginning of each iteration
    lobbyists_issuecode = g.run(
        '''CALL apoc.load.xml({f})
        YIElD value UNWIND value._children as dc
        WITH dc WHERE dc._type = 'alis'
        UNWIND dc._children as alis
        UNWIND alis._children as ali_info
        WITH ali_info WHERE ali_info._type in ['issueAreaCode','lobbyists']
        RETURN ali_info''',
        parameters={'f':fi}
    )
    # Cypher returns a cursor object, each item in the cursor object represents a issuecode and lobbyists who are associated to that issuecode

    iscd_lst = []
    # collect issuecodes of one disclosure
    lobId_collector = []

    # find_empty = False
    #find_empty data
    for i,lobbyist_issuecode in enumerate(lobbyists_issuecode):

        lobOrIss_item = lobbyist_issuecode['ali_info']
        #each item in the cursor object is a JSON object and represents a issuecode and lobbyists who are associated to that issuecode
        # Both lobbyists and issuecodes in the JSON object have attr '_type';
        # Lobbyists have attr '_children'
        # Issuecodes have attr '_text'
        if not('_text' in lobOrIss_item or '_children' in lobOrIss_item ):
            # The if statement check if an item doest not have attr '_text' nor '_children', the JSON object has no
            # issuecode data or lobbyists data to be extracted
            find_empty = True
            # here find empty data
            break

        elif '_text' in lobOrIss_item:
            # check if '_text' in the item, the item represents an issuecode
            issuecode = lobOrIss_item['_text']
            iscd_lst.append(issuecode)
            # collecting issuecodes to the idscd_lst
            issues_node = g.run(
                '''CREATE (iss:Issue {code: {code}})
                RETURN id(iss)
                ''', parameters = {'code': issuecode}
            )
            issue_id = [issue['id(iss)'] for issue in issues_node][0]
            issue_id_lst.append(issue_id)
            # Collect node'id of new (iss: Issue) created at each iteration

        elif '_children' in lobOrIss_item :
            # check if '_children' in the item, the item represents lobbyists
            #lobbyists_json = lobOrIss_item
            lobbyists = lobOrIss_item['_children']
            lob_id_lst = []

            for lobbyist in lobbyists:

                lobbyist_dic = {}
                first_name = lobbyist['_children'][0]
                last_name = lobbyist['_children'][1]
                position = lobbyist['_children'][3]

                if ('_text' in first_name) and ('_text' in last_name):

                    lobbyist_dic['firstName'] = first_name['_text']
                    lobbyist_dic['lastName'] = last_name['_text']

                    try:

                        lobbyist_dic['position'] = position['_text']
                        # position is optional in the XML file

                    except KeyError:

                        lobbyist_dic['position'] = "NULL"

                    # lobbyist_lst.append(lobbyist_dic)
                    lobbyists_node = g.run(   # Use MERGE vs CREATE: MERGE can be problematic if 2 persons have same name and position
                        ''' MERGE (lob: Lobbyist {firstName: {firstName}, lastName: {lastName}, position : {position}})
                        RETURN id(lob)
                        ''', parameters={'firstName': lobbyist_dic['firstName'],
                                         'lastName': lobbyist_dic['lastName'],
                                         'position': lobbyist_dic['position']}
                    )
                    lob_id = [lobbyist['id(lob)'] for lobbyist in lobbyists_node][0]
                    lob_id_lst.append(lob_id)
                    lobId_collector.append(lob_id)

            # print(issue_id)
            # print(lobbyist_lst)
            # print(lob_id_lst)
            lobbyist_iss_rel = g.run(
                '''Match (iss:Issue) WHERE id(iss) = {issue_id}
                MATCH (lob: Lobbyist) WHERE id(lob) in {lob_id_lst}
                CREATE (lob)-[r:LOBBIES]->(iss)
                ''', parameters={'issue_id': issue_id, 'lob_id_lst': lob_id_lst}
            )


    print(lobId_collector)
    # if find_empty:
    #     break
# ========================================== Node: Issue ==========================================#
    issuesdes = g.run(
        '''CALL apoc.load.xml({f})
        YIElD value UNWIND value._children as dl
        WITH dl WHERE dl._type = 'alis'
        UNWIND dl._children as alis
        UNWIND alis._children as ali_info
        WITH ali_info WHERE ali_info._type = 'specific_issues'
        UNWIND ali_info._children as specific_issues
        RETURN specific_issues._text''', parameters={'f': fi})

    # desc = [des['specific_issues._text'] for des in issuesdes][0]
    # print(desc) "None"


    federal_agencies = g.run(
        '''CALL apoc.load.xml({f})
        YIElD value UNWIND value._children as dl
        WITH dl WHERE dl._type = 'alis'
        UNWIND dl._children as alis
        UNWIND alis._children as ali_info
        WITH ali_info WHERE ali_info._type = 'federal_agencies'
        RETURN ali_info._text''', parameters={'f': fi})

    isde_lst = []
    feag_lst = []

    for issuedes in issuesdes:
        if issuedes['specific_issues._text'] != None :
            isde_lst.append(issuedes['specific_issues._text'])

    for agency in federal_agencies:
        if agency['ali_info._text'] != None:
            feag_lst.append(agency['ali_info._text'])
    # print(fi)
    # print(len(iscd_lst))
    # print(len(isde_lst))
    # print(len(feag_lst))

    if len(iscd_lst) == len(isde_lst) and len(isde_lst) == len(feag_lst):

        for i in range(len(iscd_lst)):
            issues = g.run(
                ''' MATCH (iss:Issue) WHERE id(iss) in {issue_id_lst}
                SET iss.des = {des}, iss.agency = {agency}
                ''', parameters={'issue_id_lst': issue_id_lst, "des" : isde_lst[i], "agency": feag_lst[i]}
            )
    else:
        print('error')


#========================================== Rel==========================================#
    dc_iss_rel = g.run(
        '''MATCH (dc:Disclosure) WHERE id(dc) = {dc_id}
        MATCH (iss:Issue) WHERE id(iss) in {iss_id}
        CREATE (dc)-[r:HAS]->(iss)
        ''',
        parameters = { 'dc_id': dc_id, 'iss_id': issue_id_lst}
    )

    lf_dc_rel = g.run(
        '''MATCH (dc:Disclosure) WHERE id(dc) = {dc_id}
        MATCH (lf:LobbyFirm) WHERE id(lf) = {lf_id}
        CREATE (lf)-[r:FILED]->(dc)
        ''',
        parameters={'dc_id': dc_id, 'lf_id': lf_id}
    )

    lob_lf_rel = g.run(
        '''MATCH (lob:Lobbyist) WHERE id(lob) in {lobId_collector}
        MATCH (lf:LobbyFirm) WHERE id(lf) = {lf_id}
        MERGE (lob)-[r:WORKS_AT]->(lf)
        ''',
        parameters={'lobId_collector': lobId_collector, 'lf_id': lf_id}
    )

    cl_dc_rel = g.run(
        '''MATCH (dc:Disclosure) WHERE id(dc) = {dc_id}
        MATCH (cl:Client) WHERE id(cl) = {cl_id}
        CREATE (cl)-[r:SIGNED]->(dc)
        ''',
        parameters={'dc_id': dc_id, 'cl_id': cl_id}
    )



# # # #===================== Node: Disclosure =====================## #===================== Node: Disclosure =====================## #===================== Node: Disclosure =====================#
# #
# #
# fi = 'file:///Users/yaqi/Documents/vir_health_graph/health-graph/data/2013_1stQuarter_XML/300529228.xml'
#
#
# #===================== Node: Disclosure =====================#
# disclosure = g.run(
#     '''CALL apoc.load.xml({f})
#     YIElD value as ld2
#     WITH [attr in ld2._children
#     WHERE attr._type in ['houseID','senateID','reportYear'] | [attr._type, attr._text]] as pairs
#     CALL apoc.map.fromPairs(pairs)
#     YIELD value
#     CREATE (dc:Disclosure {senateID: value.senateID, houseID: value.houseID, reportYear: value.reportYear})
#     RETURN id(dc)''',
#     parameters={'f': fi})
# dc_id = [dc['id(dc)'] for dc in disclosure][0]
#
# #===================== Node: Disclosure =====================#
# disclosure = g.run(
#     '''CALL apoc.load.xml({f})
#     YIElD value as ld2
#     WITH [attr in ld2._children
#     WHERE attr._type in ['houseID','senateID','reportYear'] | [attr._type, attr._text]] as pairs
#     CALL apoc.map.fromPairs(pairs)
#     YIELD value
#     CREATE (dc:Disclosure {senateID: value.senateID, houseID: value.houseID, reportYear: value.reportYear})
#     RETURN id(dc)''',
#     parameters={'f': fi})
# dc_id = [dc['id(dc)'] for dc in disclosure][0]
#
# #===================== Node: lobbyist =====================#
# lobbyists_issuecode = g.run(
#     '''CALL apoc.load.xml({f})
#     YIElD value UNWIND value._children as dl
#     WITH dl WHERE dl._type = 'alis'
#     UNWIND dl._children as alis
#     UNWIND alis._children as ali_info
#     WITH ali_info WHERE ali_info._type in ['issueAreaCode','lobbyists']
#     RETURN ali_info''', parameters={'f':fi})
#
# iscd_lst = []
# issue_id_lst = []
#
# for i,lobbyist_issuecode in enumerate(lobbyists_issuecode):
#
#     if not '_children' in lobbyist_issuecode['ali_info']:
#
#         issuecode1 = lobbyist_issuecode['ali_info']['_text']
#         iscd_lst.append(issuecode1)
#         issues = g.run(
#             '''CREATE (iss:Issue {code: {code}})
#             RETURN id(iss)
#             ''', parameters = {'code': issuecode1}
#         )
#
#         issue_id = [issue['id(iss)'] for issue in issues][0]
#         issue_id_lst.append(issue_id)
#
#     else:
#
#         lobbyists_json = lobbyist_issuecode['ali_info']
#         lobbyist_lst = []
#         lob_id_lst = []
#
#         for lobbyist in lobbyists_json['_children']:
#
#             lobbyist_dic = {}
#
#             if ('_text' in lobbyist['_children'][0]) and ('_text' in lobbyist['_children'][1]):
#
#                 lobbyist_dic['firstName'] = lobbyist['_children'][0]['_text']
#                 lobbyist_dic['lastName'] = lobbyist['_children'][1]['_text']
#
#                 try:
#
#                     lobbyist_dic['position'] = lobbyist['_children'][3]['_text']
#
#                 except KeyError:
#
#                     lobbyist_dic['position'] = "NULL"
#
#                 lobbyist_lst.append(lobbyist_dic)
#                 lobbyists = g.run(
#                     ''' MERGE (lob: Lobbyist {firstName: {firstName}, lastName: {lastName}, position : {position}})
#                     RETURN id(lob)
#                     ''', parameters={'firstName': lobbyist_dic['firstName'],
#                                      'lastName': lobbyist_dic['lastName'],
#                                      'position': lobbyist_dic['position']}
#                 )
#                 lob_id = [lobbyist['id(lob)'] for lobbyist in lobbyists][0]
#                 lob_id_lst.append(lob_id)
#
#         # print(issue_id)
#         # print(lobbyist_lst)
#         # print(lob_id_lst)
#         lobbyist_iss_rel = g.run(
#             '''Match (iss:Issue) WHERE id(iss) = {issue_id}
#             MATCH (lob: Lobbyist) WHERE id(lob) in {lob_id_lst}
#             CREATE (lob)-[r:lobbies]->(iss)
#             ''', parameters={'issue_id': issue_id, 'lob_id_lst': lob_id_lst}
#         )
#
# # ===================== Node: Issue =====================#
# issuesdes = g.run(
#     '''CALL apoc.load.xml({f})
#     YIElD value UNWIND value._children as dl
#     WITH dl WHERE dl._type = 'alis'
#     UNWIND dl._children as alis
#     UNWIND alis._children as ali_info
#     WITH ali_info WHERE ali_info._type = 'specific_issues'
#     UNWIND ali_info._children as specific_issues
#     RETURN specific_issues._text''', parameters={'f': fi})
#
# federal_agencies = g.run(
#     '''CALL apoc.load.xml({f})
#     YIElD value UNWIND value._children as dl
#     WITH dl WHERE dl._type = 'alis'
#     UNWIND dl._children as alis
#     UNWIND alis._children as ali_info
#     WITH ali_info WHERE ali_info._type = 'federal_agencies'
#     RETURN ali_info._text''', parameters={'f': fi})
#
# isde_lst = []
# feag_lst = []
#
# for issuedes in issuesdes:
#     isde_lst.append(issuedes['specific_issues._text'])
#
# for agency in federal_agencies:
#     feag_lst.append(agency['ali_info._text'])
#
# if len(iscd_lst) == len(isde_lst) and len(isde_lst) == len(feag_lst):
#
#     for i in range(len(iscd_lst)):
#         issues = g.run(
#             ''' MATCH (iss:Issue) WHERE id(iss) in {issue_id_lst}
#             SET iss.des = {des}, iss.agency = {agency}
#             ''', parameters={'issue_id_lst': issue_id_lst, "des" : isde_lst[i], "agency": feag_lst[i]}
#         )
#
# else:
#     print('error')
#
#
# # #===================== Rel=====================#
# dc_iss_rel = g.run(
#     '''MATCH (dc:Disclosure) WHERE id(dc) = {dc_id}
#     MATCH (iss:Issue) WHERE id(iss) in {iss_id}
#     CREATE (dc)-[r:has]->(iss)
#     ''',
#     parameters = { 'dc_id': dc_id, 'iss_id': issue_id_lst}
# )

#
#
#
# # tx.create(issue)
# # tx.run(disclosure)
# # tx.run(lobbyfirm)
# # tx.run(client)
# # tx.commit()
# #
# # data = open(file, 'r')
# # stng = data.read()
# # print(stng)
# # x = ET.fromstring(stng)
# # print(x)

