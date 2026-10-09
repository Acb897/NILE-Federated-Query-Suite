# ==============================
# SCARAB example: harvest a query, then answer it over what was harvested
# ==============================

import logging

from nile.scarab import scarab_harvester as scarab

# SCARAB reports its progress through the "nile.scarab" logger.
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


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

# query = """
# PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
# PREFIX bench: <http://localhost/vocabulary/bench/>
# PREFIX swrc:  <http://swrc.ontoware.org/ontology#>

# SELECT ?article
# WHERE {
#   ?article rdf:type bench:Article .
#   ?article ?property ?value
#   FILTER (?property=swrc:month)
# }
# """

# ==============================
# CONFIGURATION
# ==============================

# One or more sources. Declare the kind of each one explicitly:
#   "qpf:" or "tpf:"  a fragments server
#   "sparql:"         a SPARQL endpoint
#   "dump:"           an RDF file
sources = [
    "qpf:http://localhost:3000/kegg-sparql",
    # "qpf:http://localhost:3000/sp2bench-sparql",
    # "sparql:http://localhost:9999/blazegraph/namespace/kegg/sparql",
    # "dump:/data/caresm-diagnosis.nq",
]

# Where harvests are published and the query is evaluated.
scarab.configure(
    store_kind="remote",                    # or "local" for an embedded store (no GraphDB needed)
    store_base="http://acb8computer:7200",
    store_repository="test1",
    # export_dir="./harvests",              # also write each source's nanopublication as TriG
)


# ==============================
# HARVEST PHASE
# ==============================
# One call for all the sources. Each source is still harvested in isolation,
# into its own assertion graph, and the graphs of this run are returned.

graphs = scarab.FindBGPPriority(query, sources)

print("\nAssertion graphs of this run:")
for graph in graphs:
    print("  ", graph)

print("\nPer-source statistics:")
for stats in scarab.last_run_stats():
    decisions = [p["decision"] for p in stats["patterns"]]
    print(f"  {stats['source']}: {stats['requests']} requests, "
          f"{stats['triples_stored']} triples stored, {stats['seconds']} s, "
          f"patterns retrieved as {decisions}")


# ==============================
# EVALUATION PHASE
# ==============================
# The query is answered over this run's assertion graphs only: neither the
# provenance graphs nor the data of earlier runs can leak into the answer.

results = scarab.evaluate_over_run(query, graphs)

print("\nResults:")
print(results)

# The same in one step:
# results = scarab.answer_query(query, sources)

# Harvests exported with export_dir can be combined and answered later:
# import glob
# results = scarab.evaluate_harvest_files(query, glob.glob("./harvests/*.trig"))
