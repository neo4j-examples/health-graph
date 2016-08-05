import requests
from py2neo import Graph, Node
import os
import pandas as pd
from string_converter import remove_non_alphaNumerics as remove_marks
import numpy as np


def save_unique_txt(input, output, col, type):
    if type == 'txt':
        df = pd.read_table(input, encoding='latin-1', header=None)
    else:
        df = pd.read_csv(input, header=None)
    # df[col] = df[col].apply(str.lower)
    df_unique = df[col].drop_duplicates()
    df_unique.to_csv(output, header=None, index=None, sep=' ')

    return output

# def add_rxcui_ndc(row, g):
#     query = '''
#     MATCH (d:Drug)
#     WHERE d.drugcode = {drugcode}
#     SET d.rxcui = {rxcui}
#     '''
#     g.run(query, drugcode = row[3], rxcui = row[5])

def add_rxcui_genericDrug(row, g):
    query = '''
    MATCH (gd:GenericDrug)
    WHERE gd.genericName = {name}
    SET gd.rxcui = {rxcui}
    '''
    g.run(query, name = row[3], rxcui = row[5])

def add_rxcui_Prescription(row, g):
    query = '''
    MATCH (pc:Prescription)
    WHERE pc.genericName = {name}
    SET pc.rxcui = {rxcui}
    '''
    g.run(query, name = row[3], rxcui = row[5])
#
# def add_rxcui_name(row, g):
#     query = '''
#     MATCH (pc:Prescription)
#     WHERE pc.drugName = {drugName}
#     SET pc.rxcui = {rxcui}
#     '''
#     g.run(query, drugName = row[3], rxcui = row[5])


def float2string(data):
    return str(data)[:-2]


if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)
    tx = g.begin()

# ============================================= Extract ndc from Drug data=============================================
#     input_ndc = '/Users/yaqi/Documents/import/product.txt'
#     output_ndc = '/Users/yaqi/Documents/import/product_ndc.txt'
#     d=save_unique_txt(input_ndc,output_ndc, 1, type = 'txt')



    # input_ndc = '/Users/yaqi/Documents/import/product.txt'
    # output_ndc = '/Users/yaqi/Documents/import/product_ndc.txt'
    # d=save_unique_txt(input_ndc,output_ndc, 1, type = 'txt')

##upload the file to https://mor.nlm.nih.gov/RxMix/ to get Rxcui


# ============================================= Add rxcui to Drug =============================================
#     drug_rxcui = '/Users/yaqi/Documents/import/fb82aabeff051c0bcc486b65834dad3c.text'
#     drug_rxcui_df = pd.read_csv(drug_rxcui, sep = '|', error_bad_lines=False, skiprows = 2, header=None)
#     drug_rxcui_uniq_df = drug_rxcui_df.drop_duplicates(subset=3)
#     drug_rxcui_uniq_df[5] = drug_rxcui_uniq_df[5].apply(float2string)
#     drug_rxcui_uniq_df.apply(add_rxcui_ndc, args=(g,), axis=1)


# ============================================= Extract drug name from Prescription =============================================
#     input_dname = '/Users/yaqi/Documents/import/PartD_Prescriber_PUF_NPI_DRUG_Aa_Al_CY2013.csv'
#     output_dname = '/Users/yaqi/Documents/import/output1.txt'
#     d=save_unique_txt(input_dname,output_dname,7, type='csv')

##upload the file to https://mor.nlm.nih.gov/RxMix/ to get Rxcui

# ============================================= Add rxcui to prescription =============================================
# ## add rxcui to prescribtion:
#     rxcui_pc = '/Users/yaqi/Documents/import/8835ec230f7af301a3f02fb6dfedef71.text'
#     rxcui_pc_df = pd.read_csv(rxcui_pc, sep = '|', error_bad_lines=False, header=None)
#     rxcui_pc_df[5] = rxcui_pc_df[5].apply(float2string)
#     group_rxcui = rxcui_pc_df.groupby(3)[5].apply(list)
#
#     for name in group_rxcui.index:
#         if 'n' not in group_rxcui.loc[name]:
#             print(name, group_rxcui.loc[name])
#             query = '''
#                 MATCH (pc:Prescription)
#                 WHERE pc.drugName = {Name}
#                 SET pc.rxcui = {rxcui}
#                 WITH pc MATCH (d:Drug) WHERE d.rxcui in {rxcui}
#                 CREATE (pc)-[:PRESCRIBE]->(d)
#
#                 '''
#             g.run(query, Name=name, rxcui=group_rxcui.loc[name])

# ============================================= Create rel =============================================
#     qry = '''
#     MATCH (pc: Prescription)
#     WITH pc
#     MATCH (d:Drug) WHERE d.rxcui in pc.rxcui
#     CREATE (pc)-[:PRESCRIBE]->(d)
#     '''
#
#     g.run(qry)






# ============================================= Extract GenericName from GenericDrug============================================
#     q1 = '''
#     MATCH (gd:GenericDrug) RETURN gd.genericName'''
#
#     names = g.run(q1)
#     with open('/Users/yaqi/Documents/import/drug_genericName.txt', 'w') as text_file:
#         for name in names:
#
#             line = name['gd.genericName']+ '\n'
#             text_file.writelines(line)
            ##### split the file#####
            # print(name['gd.genericName'])
            # split - l
            # 3000
            # drug_genericName.txt

            ##### combine all files into one #####
            # cat *.text > all_rxcui.text

            ##### extract rows with rxcui #####
            # awk
            # '/\|RXCUI\|/'
            # all_rxcui.text > text.txt

# ============================================= ADD RXCUI to GenericDrug============================================
#     drug_rxcui = '/Users/yaqi/Documents/import/drug_generic/result_rxcui/result.text'
#     drug_rxcui_df = pd.read_csv(drug_rxcui, sep = '|',  header=None)
#     # print(drug_rxcui_df[5])
#     # drug_rxcui_uniq_df = drug_rxcui_df.drop_duplicates(subset=5)
#     # print(drug_rxcui_uniq_df.shape)
#
#     # drug_rxcui_uniq_df[5] = drug_rxcui_uniq_df[5].apply(str)
#     # print(drug_rxcui_df[3])
#     drug_rxcui_df.apply(add_rxcui_genericDrug, args=(g,), axis=1)


# ============================================= Extract GenericName from Prescription============================================
#     q2 = '''
#     MATCH (n:Prescription) RETURN distinct(n.genericName)'''
# #
#     names_pc = g.run(q2)
#     with open('/Users/yaqi/Documents/import/pc_genericName.txt', 'w') as text_file:
#         for name_pc in names_pc:
#
#             line_pc = name_pc['(n.genericName)']+ '\n'
#             text_file.writelines(line_pc)

# ============================================= ADD RXCUI to GenericDrug============================================
#     pc_rxcui = '/Users/yaqi/Documents/import/pc_generic/result_rxcui/result.text'
#     pc_rxcui_df = pd.read_csv(pc_rxcui, sep = '|',  header=None)
#     # print(pc_rxcui_df.shape)
#     pc_rxcui_df.apply(add_rxcui_Prescription,args=(g,), axis=1)

# ============================================= ADD rel for genericDrug, pc============================================
    q3 = '''
CALL apoc.periodic.rock_n_roll('MATCH (pc:Prescription) RETURN pc.rxcui AS pc_rx, ID(pc) AS id_pc',
'MATCH (gd:GenericDrug) WHERE gd.rxcui={pc_rx} WITH gd MATCH (pc:Prescription) WHERE ID(pc)={id_pc}  CREATE  (pc)-[:PRESCRIBE]->(gd)',100);
    '''
    g.run(q3)


