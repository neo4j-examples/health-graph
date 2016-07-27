# health-graph
![Health-Graph schema](/schema_healthGraph.png)
Click to check the ['detailed documentation'](/schema.md) of the schema. 
## Data Source

All the data for this project are avalible online and you can download or get the detailed data documentation by clicking on the link.

1. [ 'Provider Prescriptions'](https://www.cms.gov/Research-Statistics-Data-and-Systems/Statistics-Trends-and-Reports/Medicare-Provider-Charge-Data/Part-D-Prescriber.html) xlsx 

2. ['Provider Enumeration System'](http://download.cms.gov/nppes/NPI_Files.html) csv

3. ['FDA Drug Codes'](http://www.fda.gov/Drugs/InformationOnDrugs/ucm142438.htm) txt

4. ['Drug Manufacturers'](http://www.fda.gov/drugs/informationondrugs/ucm135778.htm) txt

5. ['Lobbying Disclosures'](http://disclosures.house.gov/ld/ldsearch.aspx) XML

6. ['Lobbying Contributions'](http://disclosures.house.gov/lc/lcsearch.aspx)XML

## xlsx_2_csv.py
**converts xlsx to csv**

```csv_from_excel(xls, target)```

Parameter: {xls} the path of xls/xlsx file. {target} the path of CSV file. 

The function converts xlsx file to csv format. 

## string_conerter.py
**defines functions which are used to process string values in a list of dictionary. It is part of the proccedure in creating relationship for nodes when using fuzzy string matching**

```lower_case(lst, key)```

```remove_non_alphaNumerics(lst, key)```

```string_filter(strng, stopwords)```

```sort_strings(lst, key)```

```uniq_elem(lst, key)```

## Load_disclosure.py
**ETL disclosure xml files**

get_property functions:

```
get_Disclosure_property (file)
```

```
get_LobbyFirm_proferty (file)
```

```
get_Client_property (file)
```

```
get_Issue_property(file)
```

```
get_Lobbyist_property(file)
``` 

Parameter: {file}: the path to the Lobbying Disclosures XML file.

The function calls APOC procedure ‘apoc.load.xml’ to extract child elements in the Disclosure files, then store the values in dictionary which will be passed to create_node functions as node properties.

create_node functions:
```
create_Disclousure_node (properties, file)
```
```
create_LobbyFirm_node(properties)
```
```
create_Client_node(properties)
```
```
create_Issue_node(properties)
```
```
create_lobbyist_node(properties, issueID)
```
Parameter: {file}: the path to the Lobbying Disclosures XML file. 

{properties}: a dictionary of properties. 

{issueID}: the internal Id of Issue node

The function calls Cypher CREATE or MERGE to create nodes with properties. Index are created. The function returns the internal id of the node being created. 

## Load_contribution.py
**ETL contribution xml files**

```
filer_type(file)
```
The function takes a contribution xml file to check the contribution filer type. It returns a string: either ‘L’ or ‘O’. (‘L’- lobbyist, ‘O’- Lobby Firm)

```
has_contribution(file)
```
The function takes a contribution xml file to check if the contribution is empty. It returns a boolean. 

get_property_cb functions:
```
get_LobbyFirm_property_cb (file)
```
```
get_Lobbyist_property_cb (file)
```
```
get_contribution_property_cb(file)
```
```
get_committee_property_cb(file)
```
```
get_legislator_property_cb(file)
```
Parameter: {file}: the path to the Lobbying Contribution XML files.

The function calls APOC procedure ‘apoc.load.xml’ to extract child elements in the Contribution files, then store the values in dictionary which will be passed to create_node functions as node properties.

create_NODE_node_cb functions:
```
create_LobbyFirm_node_cb(properties, file)
```
```
create_Lobbyist_node_cb(properties)
```
```
create_contribution_node_cb(property_lst)
```
```
create_committee_node(property_lst, contributionID)
```
```
create_legislator_node(property_lst, committeeID)
```
```
create_contributor_node(property, contribution_id )
```
Parameter: {file}: the path to the Lobbying Contribution XML files. 

{properties}: a dictionary of properties. 

{contributionID}: the internal Id of contribution node

The function calls Cypher CREATE or MERGE to create nodes with properties. Index are created. The function returns the internal id of the node being created.

```
contributerType(file)
```

The function takes a contribution file to extract contributor type for a contribution. The function returns a dictionary to store contributor type and contribution number. If contributor type is ‘Self’, create [:FILED{self:1}] between a filer and a contribution, a filer is either a lobbyist or lobbyFirm(refer to filer_type(file) function).  If contributor type is not ‘Self’, create a Contributor node to store this information, set [:FILED{self:0}] between a filer and a contribution. 

## Load_drug.py
**ETL drug txt file**

```
create_Drug_node(file)
```
Take the drug txt file to extract properties and create the drug node. 

## Load_drugfirm.py
**ETL drug manufacture txt file**
```
create_DrugFirm_node(file, g)
```
Take the drug manufacture txt file to extract properties and create the drug node.

## Load_prescription.py
**ETL prescription csv file**
```
create_prescription_node(file, g)
```
Take the prescription csv file to extract properties and create the prescription node.

## Load_provider.py
** ETL provider csv file**
```
create_provider_node(file, g)
```
Take the provider csv file to extract properties and create the provider node. 

## drug_df_rel.py
**Create drugfirm-[:BRANDS]->(drug) by doing fuzzy match.**
Stored drug.labelerNames, drug internal IDs in a list of dictionaries:

[{laberName1: name1, id: id1}..{laberNameX: nameX, id: idX}]

Stored drugfirm.firmName. drugfirm internal ID in a list of dictionaries:

[{firmName1: name1, id: id1}..{lfirmNameX: nameX, id: idX}]

Processed name values in these 2 lists: 
1. lowering case
2. remove non alphanumeric marks
3. chop the end with ‘s’
4. ordered words in the names

Returned unique values in the list with node id aggregated for the same value. Example: 

[{name1: id1, id2, id3.. idX}, {name2 : id5}]

Calling fuzzywussy package to do string matching between labeler name and firmname. Result is stored in array such: 

[[[id(drug1)], [id(matched drugfirm1), …,id(matched drugfirmX)]],

[[id(drug2)], [id(matched drugfirm1), …,id(matched drugfirmX)]],

...

...

[[id(drugX)], [id(matched drugfirm1), …,id(matched drugfirmX)]]]

Create relations between drug and drugFirm by using the node id. 


