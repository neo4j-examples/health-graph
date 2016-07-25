from py2neo import Graph, Node
from load_drugfirm import create_DrugFirm_node
import os
from string_converter import lower_case
from string_converter import remove_non_alphaNumerics as rm_mark
from string_converter import sort_strings
from string_converter import uniq_elem
# from collections import defaultdict
from string_converter import chop_end

if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)  ## readme need to document setting environment variable in pycharm
    tx = g.begin()

    q1 = '''
    MATCH (d: Drug)
    RETURN id(d), d.labelerName
    '''

    q2 = '''
    MATCH (df:DrugFirm)
    RETURN id(df), df.firmName'''
    drug_obj = g.run(q1)
    df_obj = g.run(q2)

    count = 0
    keys = []
    values = []
    drugs_lst = []
    for object in drug_obj:
        drug_dic = {}
        drug_dic['id'] = object['id(d)']
        drug_dic['labelerName'] = object['d.labelerName']
        drugs_lst.append(drug_dic)

    df_lst = []
    for object in df_obj:
        df_dic = {}
        df_dic['id'] = object['id(df)']
        df_dic['firmName'] = object['df.firmName']
        df_lst.append(df_dic)

    print(drugs_lst[:10])

    # [{id: 123, firmName: aaa}, {id: 345, firmName: aaa}, {id: 456, firmName: bbb}]
    # [{id: [123, 345], firmName : aaa}, {id:[456], firmName : bbb}]
    # lst = [{'id': '123', 'firmName': 'aaa'}, {'id': '345', 'firmName': 'aaa'}, {'id': '456', 'firmName': 'bbb'}]
    # d = defaultdict(list)
    # for dict in sort_fn:
    #     firmName = dict['firmName']
    #     id = dict['id']
    #     d[firmName].append(id)
    #
    # # print(d['bbb'])
    # # print(d.items())
    # print()

    nostring = ['inc', 'co', 'ltd', 'llc', 'pvt',
                'spa', 'corp', 'pty', 'og', 'kg',
                'sp', 'gp', 'lp', 'corporation', 'na',
                'lp', 'llp', 'lllp', 'lc', 'pllc',
                'pharmaceutical', 'laboratorie', 'company', 'product', 'pharma']

#lower case:
    lc_ln = lower_case(drugs_lst[:10], 'labelerName')
    lc_fn = lower_case(df_lst, 'firmName')

#remove_marks:
    rm_ln = rm_mark(lc_ln, 'labelerName')
    rm_fn = rm_mark(lc_fn, 'firmName')

#Chop_end:
    ce_ln = chop_end(rm_ln, 'labelerName', 's')
    ce_fn = chop_end(rm_fn, 'firmName', 's')
# filtr:
# filter_df = string_filter(ce_df, nostring)
# filter_dfp = string_filter(ce_dfp, nostring)
# print(filter_dfp)

#sort_strings:
    sort_ln = sort_strings(ce_ln,'labelerName')
    sort_fn = sort_strings(ce_fn, 'firmName')

#uniq strings:
    uq_ln = uniq_elem(sort_ln, 'labelerName')
    uq_fn = uniq_elem(sort_fn, 'firmName')

    # count = 0
    # for ele in uq_fn:
    #     count += 1
    # print(count)

# uq ln: 5928
# uq_fn: 7040
# ln: 106684
# fn df: 6991


#
# print(drugs_lst[:10])
#

# arr = []
# num = 0
# for i, names in enumerate(uq_dfp[:1000]):
#     labeler = names
#     result = [i, labeler]
#
#     match_company = []
#     for j, df in enumerate(uq_df):
#         company = df
#         r1 = fuzz.partial_ratio(labeler, company)
#         r2 = fuzz.ratio(labeler, company)
#
#         if r1 == 100 and r1 - r2 <= 30:
#             # print(i, j, "r1:", r1,"r2:", r2, 'labeler'+':', labeler, ' company'+':', company)
#             match_company.append(company)
#
#         elif (100 > r1 >= 95 and r2 >= 85) or (
#                     95 > r1 >= 85 and r2 >= 90):  ### miss spell or miss a word  r1 and r2 > 95
#             md_r1 = fuzz.partial_ratio(string_filter(labeler, nostring), string_filter(company, nostring))
#             md_r2 = fuzz.ratio(string_filter(labeler, nostring), string_filter(company, nostring))
#
#             if md_r1 >= 95 and md_r2 >= 95:
#                 match_company.append(company)
#                 # print(i, j, "md_r1:", md_r1,"md_r2:", md_r2, 'labeler' + ':', labeler, ' company' + ':', company)
#                 # result.append(match_company)
#     if match_company:
#         result.append(match_company)
#     else:
#         num += 1
#     arr.append(result)
# print(arr)
# print(num)





