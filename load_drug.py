from py2neo import Graph, Node
from load_drugfirm import create_DrugFirm_node
import os
from string_converter import uniq_elem


def create_Drug_node(file):
    query = '''
        USING PERIODIC COMMIT 500
        LOAD CSV WITH HEADERS FROM {file}
        AS line
        FIELDTERMINATOR '	'
        CREATE(dr:Drug {drugcode: line.PRODUCTNDC, genericName: line.NONPROPRIETARYNAME,
        tradeName: line.PROPRIETARYNAME, labelerName: line.LABELERNAME, marketing: line.MARKETINGCATEGORYNAME, DEA: line.DEASCHEDULE, startDate:line.STARTMARKETINGDATE
        })
        RETURN id(dr), dr.labelerName
        '''

    index = '''
    CREATE INDEX ON: Drug(labelerName)'''
    g.run(index)
    return g.run(query, file = file)


if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)  ## readme need to document setting environment variable in pycharm
    tx = g.begin()

    # ============================================= CREATE Drug node=============================================
    # datapath = '/Users/yaqi/Documents/data/ndctext/product.txt'
    # file = 'file:///product.txt'
    # drugs = create_Drug_node(file)

    # ============================================= CREATE DrugFirm node=============================================
    # datapath = '/Users/yaqi/Documents/data/drls_reg.txt'
    # file1 = 'file:///drls_reg.txt'
    # load_drugfirm.g = g
    # df_node = create_DrugFirm_node(file1, g)

    # ============================================= CREATE GenericDrug node=============================================
    # ============================================= CREATE GenericDrug_drug_rel node=============================================
    idx = '''CREATE INDEX ON :GenericDrug(genericName) '''
    g.run(idx)

    q2 = '''MATCH (d:Drug) WHERE EXISTS (d.genericName) RETURN id(d), lower(d.genericName)
    '''
    drugs = g.run(q2)

    drugs_lst = []
    for drug in drugs:
        drug_dic = {}
        drug_dic['id'] = drug['id(d)']
        drug_dic["genericName"] = drug['lower(d.genericName)']
        drugs_lst.append(drug_dic)
    # print(len(drugs_lst))  #106683

    uq_drug = uniq_elem(drugs_lst, 'genericName')
    # print(len(uq_drug)) #15275

    q3 = '''
    MATCH (d:Drug) where id(d) in {drug_node}
    WITH d
    MERGE (gd:GenericDrug{genericName: {genericName}})
    CREATE (d)-[:IS_GENERIC_OF]->(gd)
    '''
    num = 0
    for key in uq_drug:
        drug_node = uq_drug[key]
        genericName = key
        g.run(q3,drug_node=drug_node,genericName = genericName )
        num += 1
        print(num)


