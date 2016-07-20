from py2neo import Graph, Node
from py2neo.packages.httpstream import http
import os

def create_prescription_node(file, g):
    query = '''
    USING PERIODIC COMMIT 500
    LOAD CSV WITH HEADERS FROM 'file:///PartD_Prescriber_PUF_NPI_DRUG_Aa_Al_CY2013.csv' AS row
    CREATE (pc:prescription {npi: row.npi, drugName: row.drug_name, genericName: row.generic_name, speciality: row.specialty_description})
    FOREACH(ignoreME IN CASE WHEN trim(row.bene_count) <> "" THEN [1] ELSE [] END | SET pc.beneCount = toInt(row.bene_count))
    FOREACH(ignoreME IN CASE WHEN trim(row.total_claim_count) <> "" THEN [1] ELSE [] END | SET pc.totalClaimCount = toInt(row.total_claim_count))
    FOREACH(ignoreME IN CASE WHEN trim(row.total_drug_cost) <> "" THEN [1] ELSE [] END | SET pc.totalDrugCost = toFloat(row.total_drug_cost))
    FOREACH(ignoreME IN CASE WHEN trim(row.bene_count_ge65) <> "" THEN [1] ELSE [] END | SET pc.beneCountAge65 = toInt(row.bene_count_ge65))
    FOREACH(ignoreME IN CASE WHEN trim(row.total_claim_count_ge65) <> "" THEN [1] ELSE [] END | SET pc.totalClaimCountAge65 = toInt(row.total_claim_count_ge65))
    FOREACH(ignoreME IN CASE WHEN trim(row.total_drug_cost_ge65) <> "" THEN [1] ELSE [] END | SET pc.totalDrugCostAge65 = toFloat(row.total_drug_cost_ge65))
    '''

#    index1 = '''
#    CREATE INDEX ON: prescription(npi)
#    '''
#    index2 = '''
#        CREATE INDEX ON: prescription(drugName)
#        '''
#    index3 = '''
#        CREATE INDEX ON: prescription(genericName)
#        '''
#    g.run(index1)
#    g.run(index2)
#    g.run(index3)
    return g.run(query, file = file)


if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)  ## readme need to document setting environment variable in pycharm
    # g.delete_all()
    tx = g.begin()
    # http.socket_timeout = 9999

    # file = 'file:///Users/yaqi/Documents/Neo4j/dev/import/PartD_Prescriber_PUF_NPI_DRUG_Aa_Al_CY2013.csv'
    file = 'file:///PartD_Prescriber_PUF_NPI_DRUG_Aa_Al_CY2013.csv'
    create_prescription_node(file, g)



    # CALL apoc.load.csv({file},{sep:","}) yield map
    # USING PERIODIC COMMIT 500 LOAD CSV
    # CREATE (pc:prescription {npi: map.npi, drugName: map.drug_name, genericName: map.neneric_name,
    # totalCost: toInt(map.total_drug_cost), speciality: map.specialty_description})
    # FOREACH(ignoreME IN CASE WHEN trim(map.bene_count) <> "" THEN [1] ELSE [] END | SET pc.beneCount = map.bene_count)
    # FOREACH(ignoreME IN CASE WHEN trim(map.total_claim_count) <> "" THEN [1] ELSE [] END | SET pc.totalCalimCount = map.total_claim_count)
    # FOREACH(ignoreME IN CASE WHEN trim(map.total_drug_cost) <> "" THEN [1] ELSE [] END | SET pc.totalDrugCost = map.total_drug_cost)
    # FOREACH(ignoreME IN CASE WHEN trim(map.bene_count_ge65) <> "" THEN [1] ELSE [] END | SET pc.beneCountAge65 = map.bene_count_ge65)
    # FOREACH(ignoreME IN CASE WHEN trim(map.total_claim_count_ge65) <> "" THEN [1] ELSE [] END | SET pc.totalClaimCountAge65 = map.total_claim_count_ge65)
    # FOREACH(ignoreME IN CASE WHEN trim(map.total_drug_cost_ge65) <> "" THEN [1] ELSE [] END | SET pc.totalDrugCostAge65 = map.total_drug_cost_ge65)
    # '''



# USING PERIODIC COMMIT 500
#     LOAD CSV WITH HEADERS FROM 'file:///PartD_Prescriber_PUF_NPI_DRUG_Aa_Al_CY2013.csv' AS row
#     CREATE (pc:prescription {npi: row.npi, drugName: row.drug_name, genericName: row.generic_name, speciality: row.specialty_description})
#     FOREACH(ignoreME IN CASE WHEN trim(row.bene_count) <> "" THEN [1] ELSE [] END | SET pc.beneCount = toInt(row.bene_count))
#     FOREACH(ignoreME IN CASE WHEN trim(row.total_claim_count) <> "" THEN [1] ELSE [] END | SET pc.totalClaimCount = toInt(row.total_claim_count))
#     FOREACH(ignoreME IN CASE WHEN trim(row.total_drug_cost) <> "" THEN [1] ELSE [] END | SET pc.totalDrugCost = toFloat(row.total_drug_cost))
#     FOREACH(ignoreME IN CASE WHEN trim(row.bene_count_ge65) <> "" THEN [1] ELSE [] END | SET pc.beneCountAge65 = toInt(row.bene_count_ge65))
#     FOREACH(ignoreME IN CASE WHEN trim(row.total_claim_count_ge65) <> "" THEN [1] ELSE [] END | SET pc.totalClaimCountAge65 = toInt(row.total_claim_count_ge65))
#     FOREACH(ignoreME IN CASE WHEN trim(row.total_drug_cost_ge65) <> "" THEN [1] ELSE [] END | SET pc.totalDrugCostAge65 = toFloat(row.total_drug_cost_ge65))
#     RETURN pc LIMIT 100


