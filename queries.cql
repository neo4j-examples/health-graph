// example queries

// Top 10 popular lobbying issues in 2013

MATCH (n:Issue) RETURN n.issueAreaCode, count(n) as num order by num DESC LIMIT 10;

// Which of the lobbyists and lobby firms involved in lobbying
//Medicare and Medicaid issues in 2013, how much contribution did
//they make to government leaders and who received these contributions?

MATCH (lf:LobbyFirm)<-[:WORKS_AT]-(lob: Lobbyist)-[:LOBBIES]->(iss: Issue {issueAreaCode:'MMM'})
WITH lob, lf
MATCH (lob)-[:FILED]->(cb:Contribution)-[:MADE_TO]->(com:Committee)-[:FUNDS]->(leg:Legislator)
OPTIONAL MATCH (lf)-[:FILED]->(cb)-[:MADE_TO]->(com)-[:FUNDS]->(leg)
RETURN lf.city as City, lf.houseOrgId as Firm_ID, lf.name as Firm_Name,
lob.firstName as FirstName, lob.lastName as LastName, cb.amount as Amount, cb.date as Date, leg.name as Legislator LIMIT 50

// Relations between Pharmaceutical Research and Manufacturers of America (PhRMA) and Legislators

MATCH (cl:Client{clientName:'Pharmaceutical Research and Manufacturers of America (PhRMA)'})-[:SIGNED]->(dc:Disclosure)-[:HAS]->(iss:Issue{issueAreaCode:'MMM'})<-[:LOBBIES]-(lob:Lobbyist)-[:WORKS_AT]->(lf:LobbyFirm)
WITH cl,dc,iss,lob,lf
MATCH (lob)-[:FILED]->(cb:Contribution)-[:MADE_TO]->(com:Committee)-[:FUNDS]->(leg:Legislator)
OPTIONAL MATCH (lf)-[:FILED]->(cb)-[:MADE_TO]->(com)-[:FUNDS]->(leg)
RETURN cl,dc,iss,lob,lf,cb,com,leg LIMIT 300

//What is the most prescribed drug?

MATCH (pc:Prescription)-[:PRESCRIBE]->(gd:GenericDrug)
RETURN distinct(gd.rxcui) as drugId, gd.genericName as drugName, sum(toint(pc.totalClaimCount)) as ClaimNum
ORDER BY ClaimNum DESC limit 10

//Who prescribe the controlled drug most often?

MATCH (gd:GenericDrug)<-[:IS_GENERIC_OF]-(d:Drug)
WHERE d.DEA ='CII'
WITH gd, d
MATCH (p:Provider)-[:WRITES]->(pc:Prescription)-[:PRESCRIBE]->(gd)
RETURN distinct(p.npi) as ProviderID, gd.genericName as drugName, gd.rxcui as DrugID, sum(toint(pc.totalClaimCount)) as ClaimNum
ORDER BY ClaimNum DESC limit 10
