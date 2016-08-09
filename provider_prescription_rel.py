from py2neo import Graph, Node
import os


if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)  ## readme need to document setting environment variable in pycharm
    tx = g.begin()

    q1 = '''MATCH (p:Provider) RETURN id(p), p.npi
    '''
    providers = g.run(q1)

    #======= RETURN provider object: list of dics, key: npi, id ======#
    provider_lst = []
    for provider in providers:
        provider_dic = {}
        provider_dic['id'] = provider['id(p)']
        provider_dic['npi'] = provider['p.npi']
        provider_lst.append(provider_dic)

    # ===================== Create relation, Iterate Provider (faster, about 5000000 interations)====================#
    q2 = '''
        MATCH (p:Provider) where id(p) = {id_p}
        MATCH (pc:Prescription) where pc.npi = {p_npi}
        CREATE (p)-[:WRITES]->(pc)'''

    match_num = 0 #2407851
    for p in provider_lst:
        p_npi = p['npi']
        id_p = p['id']

        if p_npi != 'None':
            g.run(q2, p_npi=p_npi, id_p=id_p)
            match_num += 1
            print("CREATE :WRITES for Provider and Prescription:", match_num)
    print("finish create relationship")

