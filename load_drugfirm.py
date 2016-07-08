from py2neo import Graph, Node
import os

def create_DrugFirm_node(file):
    query = '''
    LOAD CSV WITH HEADERS FROM {file}
    AS line
    FIELDTERMINATOR '	'
    CREATE(df:DrugFirm {dunsNumber: line.DUNS_NUMBER, firmName: line.FIRM_NAME,
    address: line.ADDRESS, operations: line.OPERATIONS})
    '''

    index = '''
    CREATE INDEX ON: DrugFirm(firmName)'''

    g.run(index)
    return g.run(query,file = file)

if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)  ## readme need to document setting environment variable in pycharm
    g.delete_all()
    tx = g.begin()

    datapath = '/Users/yaqi/Documents/data/drls_reg.txt'
    file = 'file:///drls_reg.txt'
    df_node = create_DrugFirm_node(file)
