from py2neo import Graph
import os
from string_converter import remove_non_alphaNumerics as remove_marks
from string_converter import lower_case
from string_converter import sort_strings
from string_converter import uniq_elem
from fuzzywuzzy import fuzz


if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)
    tx = g.begin()

    idx1 = '''
    CREATE INDEX ON: Legislator(name)
    '''
    idx2 = '''
    CREATE INDEX ON: LegislatorInfo(wikipediaID)
    '''
    g.run(idx1)
    g.run(idx2)


    create_legislatorInfo = '''
    LOAD CSV WITH HEADERS
    FROM 'https://dl.dropboxusercontent.com/u/67572426/legislators-current.csv' AS line
    MERGE (legislator:LegislatorInfo { thomasID: line.thomasID })
    ON CREATE SET legislator = line
    ON MATCH SET legislator = line
    MERGE (s:State {code: line.state})
    CREATE UNIQUE (legislator)-[:REPRESENTS]->(s)
    MERGE (p:Party {name: line.currentParty})
    CREATE UNIQUE (legislator)-[:IS_MEMBER_OF]->(p)
    MERGE (b:Body {type: line.type})
    CREATE UNIQUE (legislator)-[:ELECTED_TO]->(b);
    '''
    g.run(create_legislatorInfo)
    print('create_legislatorInfo')


    rm_na_legislator = '''MATCH (l: Legislator)
    WHERE l.name in ['na','n/a', 'NA','N/A','none','None', 'N/a', 'N.A', 'N?A', 'N.A.', 'n.a']
    DETACH DELETE l
    '''
    g.run(rm_na_legislator)
    print('rm na in legislator')


    q1 = '''
    MATCH (ll:Legislator) RETURN id(ll), ll.name
    '''
    ll_obj = g.run(q1)
    ll_lst = []
    for object in ll_obj:
        ll_dic = {}
        ll_dic['id'] = object['id(ll)']
        ll_dic['name'] = object['ll.name']
        ll_lst.append(ll_dic)


    q2 = '''
    MATCH (lInfo:LegislatorInfo) RETURN id(lInfo), lInfo.wikipediaID
    '''
    lInfo_obj = g.run(q2)
    lInfo_lst = []
    for object in lInfo_obj:
        lInfo_dic = {}
        lInfo_dic['id'] = object['id(lInfo)']
        lInfo_dic['name'] = object['lInfo.wikipediaID']
        lInfo_lst.append(lInfo_dic)


    ## lowerCase:
    lc_ll = lower_case(ll_lst, 'name')
    lc_lInfo = lower_case(lInfo_lst, 'name')
    ## rm marks:
    rm_mark_ll = remove_marks(lc_ll, 'name')
    rm_mark_lInfo = remove_marks(lc_lInfo, 'name')
    ## sort names
    sort_ll = sort_strings(rm_mark_ll, 'name')
    sort_lInfo = sort_strings(rm_mark_lInfo, 'name')
    ## uniq elem:
    uq_ll = uniq_elem(sort_ll, 'name')
    uq_lInfo = uniq_elem(sort_lInfo, 'name')

    # print(len(sort_ll))  # 19825 vs 15288
    # print(len(uq_ll))

    # print(len(sort_lInfo))   # 540 vs 540
    # print(len(uq_lInfo))

    arr = []
    num = 1
    for k1 in uq_lInfo:

        num += 1
        print(num)
        name = k1
        nodeId_legislatorInfo = uq_lInfo[k1]
        i = uq_lInfo[k1]

        for k2 in uq_ll:

            match_id = {}
            ll_name = k2
            nodeId_legislator = uq_ll[k2]
            j = uq_ll[k2]
            r1 = fuzz.partial_ratio(name, ll_name)
            r2 = fuzz.ratio(name, ll_name)

            if r1 == 100 and (r1-r2) <= 70:
                match_id['id_ll'] = nodeId_legislator
                match_id['id_lInfo'] = nodeId_legislatorInfo
                match_id['r1'] = r1
                match_id['r2'] = r2
                arr.append(match_id)

    q4 = '''
        WITH {arr} as id_array
        UNWIND id_array as ids
        MATCH (ll:Legislator) WHERE id(ll) in ids.id_ll
        MATCH (lInfo:LegislatorInfo) where id(lInfo) in ids.id_lInfo
        CREATE (ll)-[r:HAS_INFO]->(lInfo)
        SET r.ratio = ids.r2, r.partial_ratio = ids.r1
        '''
    g.run(q4, arr = arr)
    print("create rel for legislator_legislatorInfo")

    # print(len(arr)) #3352


