import requests
from py2neo import Graph, Node
import os


if __name__ == "__main__":
    pw = os.environ.get('NEO4J_PASS')
    g = Graph("http://localhost:7474/", password=pw)  ## readme need to document setting environment variable in pycharm
    tx = g.begin()


    q1 = '''
    MATCH (pc: Prescription)
    RETURN id(pc), pc.drugName, pc.genericName
    '''
    pc_obj = g.run(q1)


    pc_lst = []
    for object in pc_obj:
        pc_dic = {}
        pc_dic['id'] = object['id(pc)']
        pc_dic['drugName'] = object['pc.drugName']
        pc_dic['genericName'] = object['pc.genericName']
        pc_lst.append(pc_dic)
    #
    # print(pc_lst[:40])

    # pc = [{'drugName': 'PANTOPRAZOLE SODIUM', 'genericName': 'PANTOPRAZOLE SODIUM', 'id': 4923673},
    #       {'drugName': 'VENLAFAXINE HCL ER', 'genericName': 'VENLAFAXINE HCL', 'id': 4923674},
    #       {'drugName': 'ABILIFY', 'genericName': 'ARIPIPRAZOLE', 'id': 4923675},
    #       {'drugName': 'ALENDRONATE SODIUM', 'genericName': 'ALENDRONATE SODIUM', 'id': 4923676},
    #       {'drugName': 'ALPRAZOLAM', 'genericName': 'ALPRAZOLAM', 'id': 4923677},
    #       {'drugName': 'AMBIEN', 'genericName': 'ZOLPIDEM TARTRATE', 'id': 4923678},
    #       {'drugName': 'AMBIEN CR', 'genericName': 'ZOLPIDEM TARTRATE', 'id': 4923679},
    #       {'drugName': 'AMITRIPTYLINE HCL', 'genericName': 'AMITRIPTYLINE HCL', 'id': 4923680},
    #       {'drugName': 'AMPHETAMINE SALT COMBO', 'genericName': 'DEXTROAMPHETAMINE/AMPHETAMINE', 'id': 4923681},
    #       {'drugName': 'BENZTROPINE MESYLATE', 'genericName': 'BENZTROPINE MESYLATE', 'id': 4923682},
    #       {'drugName': 'BUPROPION HCL SR', 'genericName': 'BUPROPION HCL', 'id': 4923683},
    #       {'drugName': 'BUPROPION XL', 'genericName': 'BUPROPION HCL', 'id': 4923684},
    #       {'drugName': 'BUSPIRONE HCL', 'genericName': 'BUSPIRONE HCL', 'id': 4923685},
    #       {'drugName': 'CARBAMAZEPINE', 'genericName': 'CARBAMAZEPINE', 'id': 4923686},
    #       {'drugName': 'CHLORPROMAZINE HCL', 'genericName': 'CHLORPROMAZINE HCL', 'id': 4923687},
    #       {'drugName': 'CITALOPRAM HBR', 'genericName': 'CITALOPRAM HYDROBROMIDE', 'id': 4923688},
    #       {'drugName': 'CLOMIPRAMINE HCL', 'genericName': 'CLOMIPRAMINE HCL', 'id': 4923689},
    #       {'drugName': 'CLONAZEPAM', 'genericName': 'CLONAZEPAM', 'id': 4923690},
    #       {'drugName': 'CLOZAPINE', 'genericName': 'CLOZAPINE', 'id': 4923691},
    #       {'drugName': 'CYMBALTA', 'genericName': 'DULOXETINE HCL', 'id': 4923692}]

# ============================================= add rxcui for drug in prescribtion =============================================
    url_getRxId = 'https://rxnav.nlm.nih.gov/REST/rxcui.json?'
    # url_getNdc = 'https://rxnav.nlm.nih.gov/REST/ndcproperties.json?'

    catch_no = []
    for ele in pc_lst:

        nodeId = ele['id']
        print(nodeId)

        rxnorm_dic = {}

        drug_name = {'allsrc':1,'search':2}
        generic_name = {'allsrc':1,'search':2}

        drug_name['name'] = ele['drugName']
        generic_name['name'] = ele['genericName']


        try:
            drug_rxnormId = requests.get(url_getRxId, params=drug_name).json()["idGroup"]['rxnormId'][0]

        except KeyError:
            drug_rxnormId = ''


        try:
            generic_rxnormId = requests.get(url_getRxId, params=generic_name).json()["idGroup"]['rxnormId'][0]

        except KeyError:
            generic_rxnormId = ''


        if drug_rxnormId and generic_rxnormId:
            rxnorm_dic['rxID_drug']= drug_rxnormId
            rxnorm_dic['rxID_generic'] = generic_rxnormId

        elif (not drug_rxnormId) and generic_rxnormId:
            rxnorm_dic['rxID_generic'] = generic_rxnormId

        elif (not generic_rxnormId) and drug_rxnormId:
            rxnorm_dic['rxID_drug'] = drug_rxnormId

        else:
            catch_no.append(nodeId)

        if rxnorm_dic:
            query = '''
            MATCH (pc:Prescription) where id(pc)={nodeId}
            SET pc += {dict}
            '''
            g.run(query, nodeId = nodeId, dict = rxnorm_dic)
            print(rxnorm_dic)

    print(catch_no)

