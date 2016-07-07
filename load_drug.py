from py2neo import Graph, Node
import os

def create_Drug_node(file):
    query = '''
    LOAD CSV WITH HEADERS FROM {file}
    AS line
    FIELDTERMINATOR '	'
    CREATE(dr:Drug {drugcode: line.PRODUCTNDC, genericName: line.NONPROPRIETARYNAME,
    tradeName: line.PROPRIETARYNAME, labelerName: line.LABELERNAME, marketing: line.MARKETINGCATEGORYNAME
    })
    '''

    return g.run(query,file = file)

if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)  ## readme need to document setting environment variable in pycharm
    g.delete_all()
    tx = g.begin()

    datapath = '/Users/yaqi/Documents/data/product.txt'
    file = 'file:///product.txt'

    dr_node = create_Drug_node(file)


