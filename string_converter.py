import re
import numpy as np
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

def lower_case(lst):
    '''

    :param lst: a list of strings
    :return: a new list of lowercase strings
    '''
    lower_lst = []
    for string in lst:
        lower_lst.append(string.lower())
    return lower_lst

def remove_non_alphaNumerics(lst):
    '''

    :param lst: a list of strings
    :return: a new list of strings only contain numbers and letters
    '''
    new_lst = []
    for string in lst:
        new_lst.append(re.sub(r'([^\s\w]|_)+', '', string))
    return new_lst

# def string_filter(lst, stopwords):
#     '''
#
#     :param lst:
#     :param stopwords:
#     :return:
#     '''
#     result_lst = []
#     for string in lst:
#         words = string.split()
#         result_words = [word for word in words if word not in stopwords]
#         result = ' '.join(result_words)
#         result_lst.append(result)
#     return result_lst

def string_filter(stg, stopwords):
    '''

    :param lst:
    :param stopwords:
    :return:
    '''
    words = stg.split()
    result_words = [word for word in words if word not in stopwords]
    result = ' '.join(result_words)
    return result

def sort_strings(lst):
    result_lst = []
    for string in lst:
        words = string.split()
        words.sort()
        result = ' '.join(words)
        result_lst.append(result)
    return result_lst


def uniq_elem(lst):
    result = []
    for ele in lst:
        if ele not in result:
            result.append(ele)
    return result
    # return list(set(lst))

def chop_end(lst, ending):
    result_lst = []
    for strings in lst:
        words = strings.split()
        result_string = []
        for word in words:
            if word.endswith(ending):
                word = word[:-len(ending)]
            result_string.append(word)
        result =  ' '.join(result_string)
        result_lst.append(result)
    return result_lst


if __name__ == "__main__":
    drug_firm_f = open('/Users/yaqi/Documents/import/drug_firm', 'r', encoding = "ISO-8859-1")
    drug_firm_pro_f = open('/Users/yaqi/Documents/import/drug_firm_pro', 'r', encoding = "ISO-8859-1")

    drug_firm = drug_firm_f.readlines()
    drug_firm_pro = drug_firm_pro_f.readlines()

    nostring = ['inc', 'co', 'ltd', 'llc', 'pvt',
                'spa', 'corp','pty', 'og', 'kg',
                'sp', 'gp', 'lp', 'corporation', 'na',
                'lp', 'llp', 'lllp','lc','pllc',
                'pharmaceutical', 'laboratorie', 'company', 'product', 'pharma']

# lower case:
    lc_df = lower_case(drug_firm)
    lc_dfp = lower_case(drug_firm_pro)

#remove marks:
    nomark_df = remove_non_alphaNumerics(lc_df)
    nomark_dfp = remove_non_alphaNumerics(lc_dfp)
#Chop end:
    ce_df = chop_end(nomark_df,'s')
    ce_dfp = chop_end(nomark_dfp, 's')
#filtr:
    # filter_df = string_filter(ce_df, nostring)
    # filter_dfp = string_filter(ce_dfp, nostring)
    # print(filter_dfp)

#sort:
    sort_df = sort_strings(ce_df)
    sort_dfp = sort_strings(ce_dfp)

#uniqe elem:
    # uq dfp: 5607
    # dfp: 106684
    # uq df: 6991
    # df: 10206


    uq_dfp = uniq_elem(sort_dfp)
    uq_df = uniq_elem(sort_df)

    # arr = []
    # for i, names in enumerate(uq_dfp[130:200]):
    #     labeler = names
    #     row = []
    #     for j, df in enumerate(uq_df[:500]):
    #         company = df
    #         ratio1 = fuzz.partial_ratio(labeler, company)
    #         ratio2 = fuzz.ratio(labeler, company)
    #         if ratio1 > 90:
    #             print(i, j, "r1:", ratio1,"r2:", ratio2, 'labeler'+':', labeler, ' company'+':', company)
    #         if ratio2 > 90:
    #             print(i, j, "r1:", ratio1,"r2:", ratio2, 'labeler' + ':', labeler, ' company' + ':', company)
    #         lst = [ratio1, ratio2, i, j]
    #         row.append(lst)
    #     arr.append(row)

    arr = []
    num = 0
    for i, names in enumerate(uq_dfp[:1000]):
        labeler = names
        result = [i, labeler]

        match_company = []
        for j, df in enumerate(uq_df):
            company = df
            r1 = fuzz.partial_ratio(labeler, company)
            r2 = fuzz.ratio(labeler, company)

            if r1 == 100 and r1-r2 <= 30:
                # print(i, j, "r1:", r1,"r2:", r2, 'labeler'+':', labeler, ' company'+':', company)
                match_company.append(company)

            elif (100>r1 >=95 and r2>=85) or (95>r1>=85 and r2>=90): ### miss spell or miss a word  r1 and r2 > 95
                md_r1 = fuzz.partial_ratio(string_filter(labeler, nostring), string_filter(company, nostring))
                md_r2 = fuzz.ratio(string_filter(labeler, nostring), string_filter(company, nostring))

                if md_r1 >= 95 and md_r2 >= 95:
                    match_company.append(company)
                    # print(i, j, "md_r1:", md_r1,"md_r2:", md_r2, 'labeler' + ':', labeler, ' company' + ':', company)
            # result.append(match_company)
        if match_company:
            result.append(match_company)
        else:
            num += 1
        arr.append(result)
    print(arr)
    print(num)




