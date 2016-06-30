from py2neo import Graph, Node
import os


pw = os.environ.get('NEO4J_PASS')
g= Graph("http://localhost:7474/browser/",password = pw)  ## readme need to document setting environment variable in pycharm
g.delete_all()
tx = g.begin()


#========================================== Get files ==========================================#
root =  os.getcwd()
path = os.path.join(root, "data")
contribution_MidYear = os.path.join(path, "2013_MidYear_XML")
files = [f for f in os.listdir(contribution_MidYear) if f.endswith('.xml')]
# files = ['file:///Users/yaqi/Documents/vir_health_graph/health-graph/data/2013_MidYear_XML/700653084.xml'] # Return xml files
for file in files:
    fi = 'file://' + os.path.join(contribution_MidYear, file)
    # fi = file
    print(fi)


# ========================================== Node: Contribution ==========================================#
## TODO(Yaqi add code here)
    contribution = g.run(
        '''
        '''
    )
