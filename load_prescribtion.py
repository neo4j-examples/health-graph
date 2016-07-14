from py2neo import Graph, Node
import os

def get_prescribtion_property(file, g):
    query = '''
    CALL apoc.load.csv({file},{sep:","}) yield map return * limit 5
    '''

    return g.run(query, file = file).evaluate()





# Get map
# CALL apoc.load.csv('file:///Users/yaqi/Documents/Neo4j/dev/import/PartD_Prescriber_PUF_NPI_DRUG_Aa_Al_CY2013.csv',{sep:","}) yield map return * limit 5

if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)  ## readme need to document setting environment variable in pycharm
    g.delete_all()
    tx = g.begin()

    file = 'file:///Users/yaqi/Documents/Neo4j/dev/import/PartD_Prescriber_PUF_NPI_DRUG_Aa_Al_CY2013.csv'
    print(get_prescribtion_property(file, g))

    # CALL
    # apoc.load.csv('file:///Users/yaqi/Documents/Neo4j/dev/import/PartD_Prescriber_PUF_NPI_DRUG_Aa_Al_CY2013.csv',
    #               {sep: ","})
    # yield map
    # WITH
    # map,
    # CASE
    # WA
    #
    # CREATE(pc:Prescribtion
    # {npi: map.npi, drugName: map.drug_name, genericName: map.neneric_name, totalCost: map.total_drug_cost,
    #  speciality: map.specialty_description})
    # WITH
    # map, pc, CASE
    # WHEN
    # map.bene_count <> ''
    # THEN
    # SET
    # pc.beneCount = map.bene_count
    # WHEN
    # map.total_claim_count
    # IS
    # NOT
    # NULL
    # THEN
    # set
    # pc.totalCalimCount = map.total_claim_count
    # WHEN
    # map.bene_count_ge65
    # IS
    # NOT
    # NULL
    # THEN
    # set
    # pc.beneCountAge65 = map.bene_count_ge65
    # WHEN
    # map.total_drug_cost_ge65
    # IS
    # NOT
    # NULL
    # THEN
    # set
    # pc.totalDrugCostAge65 = map.total_drug_cost_ge65
    # WHEN
    # map.total_claim_count_ge65
    # IS
    # NOT
    # NULL
    # THEN
    # set
    # pc.totalClaimCountAge65 = map.total_claim_count_ge65
    # END
    #
    # load
    # csv
    # with headers from "" as line
    # with line, case line.foo when '' then null else line.foo end as foo
    # create(:User
    # {name: line.name, foo: foo})