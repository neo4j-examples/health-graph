from py2neo import Graph, Node
from py2neo.packages.httpstream import http
import os
import csv

def create_provider_node(file, g):
    query = '''
    #USING PERIODIC COMMIT 1000
     LOAD CSV FROM {file} AS col
     CREATE (pd:Provider {npi: col[0], entityType: col[1], address: col[20]+col[21], city: col[22], state: col[23], zip: col[24], country: col[25]})
     FOREACH (row in CASE WHEN col[1]='1' THEN [1] else [] END | SET pd.firstName=col[6], pd.lastName = col[5], pd.credential= col[10], pd.gender = col[41])
     FOREACH (row in CASE WHEN col[1]='2' THEN [1] else [] END | SET pd.orgName=col[4])
    '''

    index1 = '''
    CREATE INDEX ON: Provider(npi)
    '''
    g.run(index1)

    return g.run(query, file = file)

# def remove_space_header(file):
#     with open(file, 'r') as infile:
#         reader = csv.DictReader(infile)
#         fieldnames = reader.fieldnames
#
#         for row in reader:
#             row.update({fieldname: value.strip() for (fieldname, value) in row.items()})
#     return file



if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)  ## readme need to document setting environment variable in pycharm
    # g.delete_all()
    tx = g.begin()
    # http.socket_timeout = 9999

    # file = 'file:///Users/yaqi/Documents/Neo4j/dev/import/PartD_Prescriber_PUF_NPI_DRUG_Aa_Al_CY2013.csv'
    # file = 'file:///npidata_20050523-20160612.csv'
    file = 'file:///clean.csv'
    file1 = '/Users/yaqi/Documents/Neo4j/dev/import/npidata_20050523-20160612FileHeader.csv'
    file2 = '/Users/yaqi/Documents/Neo4j/dev/import/provider.csv'
    file3 = '/Users/yaqi/Documents/Neo4j/dev/import/npidata_20050523-20160612.csv'
    file4 = 'file:///replace.csv'


    create_provider_node(file4, g)
    # new_header = []
    # with open(file3, newline='') as infile, open(file2, 'w', newline= '') as outfile:
    #     reader = csv.DictReader(infile)
    #     filednames = reader.fieldnames
    #     for names in filednames:
    #         names = names.replace(" ", "")
    #         new_header.append(names)
    #     rows = csv.reader(infile)
    #
    #     writer = csv.writer(outfile)
    #     writer.writerow(new_header)
    #     for i, row in enumerate(rows):
    #         print(i)
    #         writer.writerow(row)

    #sed '1d' npidata_20050523-20160612.csv > mynewfile.csv ####### to cut off the first line
    # Could access the title that has space using line.` `


# USING PERIODIC COMMIT 1000
#     LOAD CSV FROM 'file:///replace.csv' AS col
#     CREATE (pd:Provider {npi: col[0], entityType: col[1], address: col[20]+col[21], city: col[22], state: col[23], zip: col[24], country: col[25]})
#     FOREACH (row in CASE WHEN col[1]='1' THEN [1] else [] END | SET pd.firstName=col[6], pd.lastName = col[5], pd.credential= col[10], pd.gender = col[41])
#     FOREACH (row in CASE WHEN col[1]='2' THEN [1] else [] END | SET pd.orgName=col[4])



