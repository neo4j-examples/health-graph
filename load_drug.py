from py2neo import Graph, Node
from load_drugfirm import create_DrugFirm_node
import os

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
#     '''

    index = '''
    CREATE INDEX ON: Drug(labelerName)'''
    g.run(index)
    return g.run(query, file = file)


if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)  ## readme need to document setting environment variable in pycharm
    g.delete_all()
    tx = g.begin()

    # datapath = '/Users/yaqi/Documents/data/ndctext/product.txt'
    file = 'file:///product.txt'

    drugs = create_Drug_node(file)

    # datapath = '/Users/yaqi/Documents/data/drls_reg.txt'
    file1 = 'file:///drls_reg.txt'
    # load_drugfirm.g = g
    df_node = create_DrugFirm_node(file1, g)


    # for drug in drugs:
    #     print(drug.evaluate())
    # print(dr_node)



    # LOAD
    # CSV
    # WITH
    # HEADERS
    # FROM
    # 'file:///product.txt'
    # AS
    # line
    # FIELDTERMINATOR
    # '	'
    # CREATE(dr:Drug
    # {drugcode: line.PRODUCTNDC, genericName: line.NONPROPRIETARYNAME,
    #  tradeName: line.PROPRIETARYNAME, labelerName: line.LABELERNAME, marketing: line.MARKETINGCATEGORYNAME,
    #  DEA: line.DEASCHEDULE, startDate: line.STARTMARKETINGDATE
    #  })