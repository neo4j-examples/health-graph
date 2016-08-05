import requests
from py2neo import Graph, Node
import os
import pandas as pd
from string_converter import remove_non_alphaNumerics as remove_marks
import numpy as np


if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)
    tx = g.begin()

#===================== RETURN GenericDrug object: list of dics, key: rxcui, id =====================#
    q1 = '''
    MATCH (gd:GenericDrug) RETURN id(gd), gd.rxcui
    '''
    gd_obj=g.run(q1)

    gd_lst = []
    for object in gd_obj:
        gd_dic = {}
        gd_dic['id'] = object['id(gd)']
        gd_dic['rxcui'] = object['gd.rxcui']
        gd_lst.append(gd_dic)
    pc_lst = []

#===================== Create relation, Iterate genericDrug (faster, about 4000+ interations)====================#
    q3 = '''
       MATCH (pc:Prescription) where pc.rxcui = {gd_rxcui}
       MATCH (gd:GenericDrug) where id(gd) = {id_gd}
       CREATE (pc)-[:PRESCRIBE]->(gd)
       '''
    match_num = 0
    for gd in gd_lst:
        gd_rxcui = gd['rxcui']
        id_gd = gd['id']
        if gd_rxcui != 'None':
            g.run(q3, gd_rxcui = gd_rxcui, id_gd = id_gd)
            match_num += 1
            print("CREATE :PRESCRIBE for Prescription and GenericDrug:", match_num)

    print("finish create relationship: ")


    #===================== Create relation, Iterate Prescription (slow, about 22,941,414 interations) =====================#
    # q2 = '''
    # MATCH (pc:Prescription) RETURN id(pc), pc.rxcui
    # '''
    # pc_obj = g.run(q2)
    #
    # for object in pc_obj:
    #     pc_dic = {}
    #     pc_dic['id'] = object['id(pc)']
    #     pc_dic['rxcui'] = object['pc.rxcui']
    #     pc_lst.append(pc_dic)
    #
    # q4 = '''
    #    MATCH (gd:GenericDrug) where gd.rxcui = {pc_rxcui}
    #    MATCH (pc:Prescription) where id(pc) = {id_pc}
    #    CREATE (pc)-[:PRESCRIBE]->(gd)'''
    #
    # match_num = 0
    # unmatch_num = 0
    # for pc in pc_lst:
    #     pc_rxcui = pc['rxcui']
    #     id_pc = pc['id']
    #     if pc_rxcui != 'None':
    #         g.run(q4, pc_rxcui = pc_rxcui, id_pc = id_pc)
    #         match_num += 1
    #         print("match_num:", match_num)
    #     else:
    #         unmatch_num += 1
    #         print("unmatch_num:", unmatch_num)
    # print("match_num:", match_num)
    # print("unmatch_num:", unmatch_num)


