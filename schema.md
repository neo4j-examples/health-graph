
# health-graph
### Drug-Manufacture-Lobby-Prescription Graph ###
(This file is in development)

#### Nodes ####

##### (drug) #####

* ndc: National Drug Code = labeler code-product code-package code
* brand_name
* generic_name
* start_marketing_date:
* ['marketing_category'](http://www.fda.gov/ForIndustry/DataStandards/StructuredProductLabeling/ucm162528.htm)
* dea_schedule :  drug’s acceptable medical use and the drug’s abuse or dependency potential
* pharm_classes

##### (manufacture) #####

* labeler_code
* firm_name
* address
* operations

##### (provider) #####

* npi: National Provider Identifier
* first_name
* last_name
* credential
* specialty
* gender
* npi_deactivation_date
* npi_reactivation_date

##### (prescription) #####

* bene_count: the total number of unique PartD beneficiaries ( <11 is not counted)
* total_claim_count: Original prescription + refills (<11 is not counted)
* cost: total drug cost
* year: so far this is 2013

##### (hospital) #####

* name
* address
* zipcode
* city
* state
* Country

##### (Lobby_firm) ##### 
* Organization_name:
* Organization_address:
* Organization_city:  
* Organization_state
* Organization_country
* Organization_zipcode
* Organization_house_ID
* Organization_senate_ID

##### (lobbyist) ##### 
* First_name  
* Last_name

##### (Disclosure) ##### 
* Report_year:
* House_ID: Organization_house_ID + client ID
* file_id

##### (Issue) ##### 
* file_id
* issue _area_code:
* Description:

##### (Contribution) ##### 
* house_ID: organization_house ID
* Countribution_amount:
* payee_name:
* Recipim_name: 

##### Relationships ##### 
* (provider)-[:writes]->(prescription)  (the prescription contains aggregated properties)  
* (provider)-[:works_at]->(hospital)
* (prescription)-[:refers_to]->(drug)
* (manufacture)-[:produces]->(drug) 
* (manufacture)-[:hired]->(lobby_firm) 
* (lobby_firm)-[:filed]->(disclosure) 
* (disclosure)-[:represents]->(client)
* (disclosure)-[:regards-to]->(issue)
* (lobbyist)-[:lobbies]->(issue)
* (lobbyist)-[:work_at]->(
* (lobby_firm)-[:made]->(contribution)
* (legislator)-[:received]->(contribution)












