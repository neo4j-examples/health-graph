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

    #======= RETURN Drug object: list of dics, key: labelerName, id ======#
    q1 = '''
    MATCH (d: Drug)
    RETURN id(d), d.labelerName
    '''
    drug_obj = g.run(q1)
    drugs_lst = []
    for object in drug_obj:
        drug_dic = {}
        drug_dic['id'] = object['id(d)']
        drug_dic['labelerName'] = object['d.labelerName']
        drugs_lst.append(drug_dic)

    #======= RETURN DrugFirm object: list of dics, key: firmName, id ======#
    q2 = '''
    MATCH (df:DrugFirm)
    RETURN id(df), df.firmName'''
    df_obj = g.run(q2)
    df_lst = []
    for object in df_obj:
        df_dic = {}
        df_dic['id'] = object['id(df)']
        df_dic['firmName'] = object['df.firmName']
        df_lst.append(df_dic)

    #======= Processing list of strings (laberlerName, firmName) ======#
    nostring = ['inc', 'co', 'ltd', 'llc', 'pvt',
                'spa', 'corp', 'pty', 'og', 'kg',
                'sp', 'gp', 'lp', 'corporation', 'na',
                'lp', 'llp', 'lllp', 'lc', 'pllc',
                'pharmaceutical', 'laboratorie', 'company', 'product', 'pharma']

    #lower case:
    lc_ln = lower_case(drugs_lst, 'labelerName')
    lc_fn = lower_case(df_lst, 'firmName')

    #remove_marks:
    rm_ln = rm_mark(lc_ln, 'labelerName')
    rm_fn = rm_mark(lc_fn, 'firmName')

    #Chop_end:
    ce_ln = chop_end(rm_ln, 'labelerName', 's')
    ce_fn = chop_end(rm_fn, 'firmName', 's')

    #sort_strings:
    sort_ln = sort_strings(ce_ln,'labelerName')
    sort_fn = sort_strings(ce_fn, 'firmName')

    #uniq strings:
    uq_ln = uniq_elem(sort_ln, 'labelerName')
    uq_fn = uniq_elem(sort_fn, 'firmName')

    #======= Create relation :BRANDS (String Fuzzying Matching) ======#
    q3 = '''
    MATCH (d:Drug) where id(d) in {drug_id} and d.tradeName is not NULL
    MATCH (df:DrugFirm) where id(df) in {drug_firm_id}
    MERGE (df)-[r:BRANDS]->(d)
    ON CREATE SET r.ratio = {r2}, r.partial_ratio = {r1}'''

    num = 0
    for k1 in uq_ln:

        labeler_name = k1
        nodeId_drug = uq_ln[k1]

        for k2 in uq_fn:

            company_name = k2
            nodeId_df = uq_fn[k2]
            r1 = fuzz.partial_ratio(labeler_name, company_name)
            r2 = fuzz.ratio(labeler_name, company_name)

            if r1 == 100 and (r1 - r2) <= 30:

                g.run(q3, drug_id = nodeId_drug, drug_firm_id = nodeId_df, r1 = r1, r2 = r2)
                num += 1
                print("CREATE relation :BRANDS number:", num)

            elif (100 > r1 >= 95 and r2 >= 85) or (95 > r1 >= 85 and r2 >= 90):  ### miss spell or miss a word  r1 and r2 > 95

                md_r1 = fuzz.partial_ratio(string_filter(labeler_name, nostring), string_filter(company_name, nostring))
                md_r2 = fuzz.ratio(string_filter(labeler_name, nostring), string_filter(company_name, nostring))

                if md_r1 >= 95 and md_r2 >= 95:

                    g.run(q3, drug_id=nodeId_drug, drug_firm_id=nodeId_df, r1=md_r1, r2=md_r2)
                    num += 1
                    print("CREATE relation :BRANDS rel number:", num)

    print("finish creating relation for :Drug and :DrugFirm")








