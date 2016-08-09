from py2neo import Graph, Node
from load_drugfirm import create_DrugFirm_node
import os
from string_converter import lower_case
from string_converter import remove_non_alphaNumerics as rm_mark
from string_converter import sort_strings
from string_converter import uniq_elem
from string_converter import chop_end
from string_converter import string_filter
from fuzzywuzzy import fuzz


if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)  ## readme need to document setting environment variable in pycharm
    tx = g.begin()

    q1 = '''
    MATCH (cl: Client)
    RETURN id(cl), cl.clientName
    '''

    q2 = '''
    MATCH (df:DrugFirm)
    RETURN id(df), df.firmName'''

    client_obj = g.run(q1)
    df_obj = g.run(q2)

    client_lst = []
    for client in client_obj:
        client_dic = {}
        client_dic['id'] = client['id(cl)']
        client_dic['clientName'] = client['cl.clientName']
        client_lst.append(client_dic)

    df_lst = []
    for object in df_obj:
        df_dic = {}
        df_dic['id'] = object['id(df)']
        df_dic['firmName'] = object['df.firmName']
        df_lst.append(df_dic)


    nostring = ['inc', 'co', 'ltd', 'llc', 'pvt',
                'spa', 'corp', 'pty', 'og', 'kg',
                'sp', 'gp', 'lp', 'corporation', 'na',
                'lp', 'llp', 'lllp', 'lc', 'pllc',
                'pharmaceutical', 'laboratorie', 'company', 'product', 'pharma']

#lower case:
    lc_cn = lower_case(client_lst, 'clientName')
    lc_fn = lower_case(df_lst, 'firmName')

#remove_marks:
    rm_cn = rm_mark(lc_cn, 'clientName')
    rm_fn = rm_mark(lc_fn, 'firmName')

#Chop_end:
    ce_cn = chop_end(rm_cn, 'clientName', 's')
    ce_fn = chop_end(rm_fn, 'firmName', 's')

#sort_strings:
    sort_cn = sort_strings(ce_cn,'clientName')
    sort_fn = sort_strings(ce_fn, 'firmName')

#uniq strings:
    uq_cn = uniq_elem(sort_cn, 'clientName')
    uq_fn = uniq_elem(sort_fn, 'firmName')
#
    # print(len(uq_cn))
    # print(len(client_lst))
    #
    # print(len(uq_fn))
    # print(len(df_lst))
#
# # uq cn: 15381
# # uq_fn: 7040
# # client name: 17047
# # fn df:10205
#
    # arr = []
    result = []
    num = 0
    count = 0

    q3 = '''MATCH (df:DrugFirm) WHERE id(df) in {nodeId_drugFirm}
    MATCH (cl:Client) WHERE id(cl) in {nodeId_client}
    MERGE (cl)-[r:SIMILAR_NAME]->(df)
    SET r.ratio = {r2}, r.partial_ratio = {r1}
    '''

    for k1 in uq_fn:

        firm_name = k1
        nodeId_drugFirm = uq_fn[k1]
        i = uq_fn[k1]

        for k2 in uq_cn:

            client_name = k2
            nodeId_client = uq_cn[k2]
            j = uq_cn[k2]
            r1 = fuzz.partial_ratio(firm_name, client_name)
            r2 = fuzz.ratio(firm_name, client_name)


            if r1 == 100 and (r1 - r2) <= 30:
                print(i, j, "r1:", r1,"r2:", r2, 'firmName'+':', firm_name, ' client'+':', client_name)
                g.run(q3, nodeId_drugFirm = nodeId_drugFirm, nodeId_client = nodeId_client, r2 = r2, r1 = r1 )

            elif (100 > r1 >= 95 and r2 >= 85) or (95 > r1 >= 85 and r2 >= 90):  ### miss spell or miss a word  r1 and r2 > 95
                md_r1 = fuzz.partial_ratio(string_filter(firm_name, nostring), string_filter(client_name, nostring))
                md_r2 = fuzz.ratio(string_filter(firm_name, nostring), string_filter(client_name, nostring))

                if md_r1 >= 95 and md_r2 >= 95:
                    print(i, j, "md_r1:", md_r1, "md_r2:", md_r2, 'firmName' + ':', firm_name, ' client' + ':', client_name)
                    g.run(q3, nodeId_drugFirm=nodeId_drugFirm, nodeId_client=nodeId_client, r2=md_r2, r1=md_r1)

