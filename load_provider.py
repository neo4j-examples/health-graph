from py2neo import Graph, Node
import os

def create_provider_node(file, g):
    query = '''
     USING PERIODIC COMMIT 1000
     LOAD CSV FROM {file} AS col
     CREATE (pd:Provider {npi: col[0], entityType: col[1], address: col[20]+col[21], city: col[22], state: col[23], zip: col[24], country: col[25]})
     FOREACH (row in CASE WHEN col[1]='1' THEN [1] else [] END | SET pd.firstName=col[6], pd.lastName = col[5], pd.credential= col[10], pd.gender = col[41])
     FOREACH (row in CASE WHEN col[1]='2' THEN [1] else [] END | SET pd.orgName=col[4])
    '''

    return g.run(query, file = file)


if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)  ## readme need to document setting environment variable in pycharm
    tx = g.begin()
    print('graph begin')

    index1 = '''
    CREATE INDEX ON: Provider(npi)
    '''
    g.run(index1)

    file = 'file:///provider_clean.csv'
    print('Start creating provider')
    create_provider_node(file, g)
    print('created provider')

    #sed '1d' npidata_20050523-20160612.csv > mynewfile.csv ####### to cut off the first line
    # Could access the title that has space using line.` `





