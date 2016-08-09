import requests
from py2neo import Graph, Node
import os
import pandas as pd
from string_converter import remove_non_alphaNumerics as remove_marks
import numpy as np


def add_rxcui_GenericDrug(row, g):
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


if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)
    tx = g.begin()


# # ============================================= Extract GenericName from Prescription Data============================================
#     #====== in bash =======#
#     #==== combine all files into one:
#             # $cat *.csv > all_prescription.csv
#
#     dtype = {'npi': str, 'nppes_provider_last_org_name': str, 'nppes_provider_first_name': str,
#        'nppes_provider_city':str, 'nppes_provider_state':str, 'specialty_description':str,
#        'description_flag':str, 'drug_name':str, 'generic_name':str, 'bene_count':str,
#        'total_claim_count':str, 'total_day_supply':str, 'total_drug_cost':str,
#        'bene_count_ge65':str, 'bene_count_ge65_redact_flag':str,
#        'total_claim_count_ge65':str, 'ge65_redact_flag':str, 'total_day_supply_ge65':str,
#        'total_drug_cost_ge65':str}
#
#     # all_pc = '''/Users/yaqi/Documents/Data/PartD_Prescriber/prescription_csv/all_prescription.csv'''
#     # output = '''/Users/yaqi/Documents/Data/PartD_Prescriber/prescription_csv/pc_uniq_gn.txt'''
#
#     all_pc = '''/Users/yaqi/Documents/Neo4j/load_pc_drug_df/import/all_prescription.csv'''
#     output = '''/Users/yaqi/Documents/Neo4j/load_pc_drug_df/import/pc_uniq_gn.txt'''
#
#     df = pd.read_csv(all_pc, dtype = dtype)
#     # print(df.shape)  (23584079, 19)
#     genericName = df['generic_name']
#     uniq_genericName = genericName.drop_duplicates()
#     uniq_genericName.to_csv(output, header=None, index=None, sep=' ')
#
#     # ====== in bash =======#
#     # cd to import directory
#
#     # Send pc_uniq_gn.txt data to https://mor.nlm.nih.gov/RxMix/
#     # Select Function:  findRxcuiByString
#     # Optional Parameters:
#         # source_type: 13 selected
#         # allSourcesFlag: 1
#         # searchType : 2
#     # output format: TEXT

# ============================================= ADD RXCUI to node Prescription ============================================
    # save return text file in improt directory
    #====== in bash: extract rows with rxcui  =======#
    # cd to db/import directory
    # $awk '/\|RXCUI\|/' 4d0f0dc897a98b673797bcddf13c1db3.text > rxcui_pc.txt

    pc_rxcui = '/Users/yaqi/Documents/Neo4j/load_pc_drug_df copy/import/rxcui_pc.txt'
    pc_rxcui_df = pd.read_csv(pc_rxcui, sep = '|',  header=None)

    idx = '''CREATE INDEX ON :Prescription(rxcui) '''
    g.run(idx)

    pc_rxcui_df.apply(add_rxcui_Prescription,args=(g,), axis=1)
    print("finish adding rxcui to :Prescription")

    # print(genericName.shape) (23584079,)

# ============================================= Extract GenericName from GenericDrug============================================
#     q1 = '''
#     MATCH (gd:GenericDrug) RETURN gd.genericName'''
#
#     names = g.run(q1)
#     with open('/Users/yaqi/Documents/Neo4j/load_pc_drug_df/import/drug_genericName.txt', 'w') as text_file:
#         for name in names:
#
#             line = name['gd.genericName']+ '\n'
#             text_file.writelines(line)
#
#     # ====== in bash: split large file to small ones  =======#
#     # cd to db/import directory
#     #split the file#
#             # $split - l 3000 drug_genericName.txt
#
#     # Send splited txt files to https://mor.nlm.nih.gov/RxMix/
#     # Select Function:  findRxcuiByString
#     # Optional Parameters:
#         # source_type: 13 selected
#         # allSourcesFlag: 1
#         # searchType : 2
#     # output format: TEXT

# ============================================= ADD RXCUI to GenericDrug============================================
    # save return text files in improt directory
    #====== in bash: extract rows with rxcui  =======#
    # cd to db/import directory
    #combile all file into one:
    # $cat *.text > all_rxcui.text
    # $awk '/\|RXCUI\|/'all_rxcui.text > rxcui_gd.txt

    drug_rxcui = '/Users/yaqi/Documents/Neo4j/load_pc_drug_df copy/import/rxcui_gd.text'
    drug_rxcui_df = pd.read_csv(drug_rxcui, sep = '|',  header=None)
    drug_rxcui_uniq_df = drug_rxcui_df.drop_duplicates(subset=5)

    idx1 = '''CREATE INDEX ON :GenericDrug(rxcui) '''
    g.run(idx1)

    drug_rxcui_df.apply(add_rxcui_GenericDrug, args=(g,), axis=1)
    print("finish adding rxcui to :GenericDrug")

