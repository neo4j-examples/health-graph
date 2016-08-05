import re
import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from collections import defaultdict


def lower_case(lst, key):
    '''

    :param lst: a list of dic: {strings: value, id: value}
    :return: a new list of lowercase strings
    '''
    result = []
    for ele in lst:
        dict = {}
        strings = ele[key]
        dict[key] = strings.lower()
        dict['id'] = ele['id']
        result.append(dict)
    return result


def remove_non_alphaNumerics(lst, key):
    '''

    :param lst: a list of strings
    :return: a new list of strings only contain numbers and letters
    '''
    result = []
    for ele in lst:
        dict = {}
        strings = ele[key]
        dict[key] = re.sub(r'([^\s\w]|_)+', '', strings)
        dict['id'] = ele['id']
        result.append(dict)
    return result


def string_filter(strng, stopwords):
    '''

    :param lst:
    :param stopwords:
    :return:
    '''
    words = strng.split()
    result_words = [word for word in words if word not in stopwords]
    result = ' '.join(result_words)
    return result


def sort_strings(lst, key):
    result = []
    for ele in lst:
        dict = {}
        strings = ele[key]
        words = strings.split()
        words.sort()
        dict[key] = ' '.join(words)
        dict['id'] = ele['id']
        result.append(dict)
    return result


def uniq_elem(lst, key):
    '''

    :param lst: a list of dic: {strings: value, id: value}
    :param key:
    :return:
    '''

    result = defaultdict(list)
    for dict in lst:
        name = dict[key]
        id = dict['id']
        result[name].append(id)
    return result


def chop_end(lst, key, ending):
    result_lst = []
    for ele in lst:
        dict = {}
        strings = ele[key]
        words = strings.split()
        result_string = []
        for word in words:
            if word.endswith(ending):
                word = word[:-len(ending)]
            result_string.append(word)
        result =  ' '.join(result_string)
        dict[key] = result
        dict['id'] = ele['id']
        result_lst.append(dict)
    return result_lst


# if __name__ == "__main__":
#     drug_firm_f = open('/Users/yaqi/Documents/import/drug_firm', 'r', encoding = "ISO-8859-1")
#     drug_firm_pro_f = open('/Users/yaqi/Documents/import/drug_firm_pro', 'r', encoding = "ISO-8859-1")
#
#     drug_firm = drug_firm_f.readlines()
#     drug_firm_pro = drug_firm_pro_f.readlines()
#
#     nostring = ['inc', 'co', 'ltd', 'llc', 'pvt',
#                 'spa', 'corp','pty', 'og', 'kg',
#                 'sp', 'gp', 'lp', 'corporation', 'na',
#                 'lp', 'llp', 'lllp','lc','pllc',
#                 'pharmaceutical', 'laboratorie', 'company', 'product', 'pharma']
#
# # lower case:
#     lc_df = lower_case(drug_firm)
#     lc_dfp = lower_case(drug_firm_pro)
#
# #remove marks:
#     nomark_df = remove_non_alphaNumerics(lc_df)
#     nomark_dfp = remove_non_alphaNumerics(lc_dfp)
# #Chop end:
#     ce_df = chop_end(nomark_df,'s')
#     ce_dfp = chop_end(nomark_dfp, 's')
# #filtr:
#     # filter_df = string_filter(ce_df, nostring)
#     # filter_dfp = string_filter(ce_dfp, nostring)
#     # print(filter_dfp)
#
# #sort:
#     sort_df = sort_strings(ce_df)
#     sort_dfp = sort_strings(ce_dfp)
#
# #uniqe elem:
#     # uq dfp: 5607
#     # dfp: 106684
#     # uq df: 6991
#     # df: 10206
#
#
#     uq_dfp = uniq_elem(sort_dfp)
#     uq_df = uniq_elem(sort_df)
#
#
#
#     # arr = []
#     # for i, names in enumerate(uq_dfp[130:200]):
#     #     labeler = names
#     #     row = []
#     #     for j, df in enumerate(uq_df[:500]):
#     #         company = df
#     #         ratio1 = fuzz.partial_ratio(labeler, company)
#     #         ratio2 = fuzz.ratio(labeler, company)
#     #         if ratio1 > 90:
#     #             print(i, j, "r1:", ratio1,"r2:", ratio2, 'labeler'+':', labeler, ' company'+':', company)
#     #         if ratio2 > 90:
#     #             print(i, j, "r1:", ratio1,"r2:", ratio2, 'labeler' + ':', labeler, ' company' + ':', company)
#     #         lst = [ratio1, ratio2, i, j]
#     #         row.append(lst)
#     #     arr.append(row)
#
#     arr = []
#     num = 0
#     for i, names in enumerate(uq_dfp[:1000]):
#         labeler = names
#         result = [i, labeler]
#
#         match_company = []
#         for j, df in enumerate(uq_df):
#             company = df
#             r1 = fuzz.partial_ratio(labeler, company)
#             r2 = fuzz.ratio(labeler, company)
#
#             if r1 == 100 and r1-r2 <= 30:
#                 # print(i, j, "r1:", r1,"r2:", r2, 'labeler'+':', labeler, ' company'+':', company)
#                 match_company.append(company)
#
#             elif (100>r1 >=95 and r2>=85) or (95>r1>=85 and r2>=90): ### miss spell or miss a word  r1 and r2 > 95
#                 md_r1 = fuzz.partial_ratio(string_filter(labeler, nostring), string_filter(company, nostring))
#                 md_r2 = fuzz.ratio(string_filter(labeler, nostring), string_filter(company, nostring))
#
#                 if md_r1 >= 95 and md_r2 >= 95:
#                     match_company.append(company)
#                     # print(i, j, "md_r1:", md_r1,"md_r2:", md_r2, 'labeler' + ':', labeler, ' company' + ':', company)
#             # result.append(match_company)
#         if match_company:
#             result.append(match_company)
#         else:
#             num += 1
#         arr.append(result)
#     print(arr)
#     print(num)
#
#
#
#
#



