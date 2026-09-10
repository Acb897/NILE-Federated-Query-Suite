# ==============================
# Import functions
# ==============================

from nile.scarab.scarab_harvester import FindBGPPriority, execute_sparql_query, fetch_tpf_page


# ==============================
# Example SPARQL Query
# ==============================

# query = """
# PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>
# PREFIX dbpprop: <http://dbpedia.org/property/>
# PREFIX dc: <http://purl.org/dc/terms/>
# PREFIX dbpedia: <http://dbpedia.org/resource/Category:>

# SELECT ?person ?city WHERE {
#   ?person a dbpedia-owl:Architect .
#   ?person dbpprop:birthPlace ?city .
#   ?city dc:subject dbpedia:Capitals_in_Europe .
# }
# """

# query = """
# SELECT ?drugDesc ?cpd ?equation WHERE {
#    ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/drugCategory> <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugcategory/cathartics> .
#    ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/keggCompoundId> ?cpd .
#    ?drug <http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/description> ?drugDesc .
#    ?enzyme <http://bio2rdf.org/ns/kegg#xSubstrate> ?cpd .
#    ?enzyme <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://bio2rdf.org/ns/kegg#Enzyme> .
#    ?reaction <http://bio2rdf.org/ns/kegg#xEnzyme> ?enzyme .
#    ?reaction <http://bio2rdf.org/ns/kegg#equation> ?equation . 
# }
# """

query = """
SELECT ?enzyme ?reaction ?equation WHERE {
   ?enzyme <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://bio2rdf.org/ns/kegg#Enzyme> .
   ?reaction <http://bio2rdf.org/ns/kegg#xEnzyme> ?enzyme .
   ?reaction <http://bio2rdf.org/ns/kegg#equation> ?equation . 
}
"""

# ==============================
# CONFIGURATION
# ==============================

named_graph_iri = "http://example.org/graphs/federated-temp"

# Provide ONE or MANY endpoints
tpf_servers = [
    "http://localhost:3000/kegg-sparql",
    # Add more endpoints here
    # "https://example.org/endpointA",
    # "https://example.org/endpointB"
]

# Ensure it's always an array
if not isinstance(tpf_servers, list):
    tpf_servers = [tpf_servers]


# ==============================
# FEDERATED HARVEST PHASE
# ==============================

print("=======================================")
print("Starting Federated Harvest")
print("=======================================")

for endpoint in tpf_servers:

    print(f"\n--- Checking endpoint: {endpoint} ---")

    try:
        # This function should:
        # - Detect matching triple patterns
        # - Harvest triples
        # - Insert them into local GraphDB
        FindBGPPriority(query, endpoint)

    except Exception as e:
        print(f"Error processing endpoint {endpoint}: {e}")


# ==============================
# LOCAL QUERY EXECUTION PHASE
# ==============================

print("\n=======================================")
print("Executing query over local GraphDB")
print("=======================================")

results = execute_sparql_query(query)

print("\nResults:")
print(results)

# # Test call in main.py or separately
# test_url = "http://localhost:3000/kegg-sparql?subject=http%3A%2F%2Fbio2rdf.org%2Fcpd%3AC00006"
# g = fetch_tpf_page(test_url)
# print("Test parse result:", len(g))