from py2neo import Graph, Node
from py2neo.packages.httpstream import http
import os

def create_prescription_node(file, g):
    query = '''
    USING PERIODIC COMMIT 500
    LOAD CSV WITH HEADERS FROM {file} AS row
    CREATE (pc:Prescription {npi: row.npi, drugName: row.drug_name, genericName: row.generic_name, speciality: row.specialty_description})
    FOREACH(ignoreME IN CASE WHEN trim(row.bene_count) <> "" THEN [1] ELSE [] END | SET pc.beneCount = toInt(row.bene_count))
    FOREACH(ignoreME IN CASE WHEN trim(row.total_claim_count) <> "" THEN [1] ELSE [] END | SET pc.totalClaimCount = toInt(row.total_claim_count))
    FOREACH(ignoreME IN CASE WHEN trim(row.total_drug_cost) <> "" THEN [1] ELSE [] END | SET pc.totalDrugCost = toFloat(row.total_drug_cost))
    FOREACH(ignoreME IN CASE WHEN trim(row.bene_count_ge65) <> "" THEN [1] ELSE [] END | SET pc.beneCountAge65 = toInt(row.bene_count_ge65))
    FOREACH(ignoreME IN CASE WHEN trim(row.total_claim_count_ge65) <> "" THEN [1] ELSE [] END | SET pc.totalClaimCountAge65 = toInt(row.total_claim_count_ge65))
    FOREACH(ignoreME IN CASE WHEN trim(row.total_drug_cost_ge65) <> "" THEN [1] ELSE [] END | SET pc.totalDrugCostAge65 = toFloat(row.total_drug_cost_ge65))
    '''

    index1 = '''
    CREATE INDEX ON: Prescription(npi)
    '''

    index2 = '''
    CREATE INDEX ON: Prescription(genericName)
    '''
    g.run(index1)
    g.run(index2)
    return g.run(query, file = file)


if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)  ## readme need to document setting environment variable in pycharm
    tx = g.begin()
    # http.socket_timeout = 9999

    # file = 'file:///Users/yaqi/Documents/Neo4j/dev/import/PartD_Prescriber_PUF_NPI_DRUG_Aa_Al_CY2013.csv'
    # file = 'file:///PartD_Prescriber_PUF_NPI_DRUG_Aa_Al_CY2013.csv'
    # create_prescription_node(file, g)

    root =  '/Users/yaqi/Documents/Neo4j/load_pc_drug_df1/import/'
    filenames = [f for f in os.listdir(root) if f.endswith('.csv')]

    for file in filenames:
        filename = 'file:///'+file
        create_prescription_node(filename, g)
        print('finish loading:', file)







