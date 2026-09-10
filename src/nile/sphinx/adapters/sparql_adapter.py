"""Indexing a repository through its SPARQL endpoint."""

class SPARQLAdapter:
    """Explores a repository by querying its SPARQL endpoint.

    The simplest of the three adapters: each exploration step is a
    single SPARQL query, built and executed by the Engine that owns the
    adapter. Those queries adapt themselves to wherever the repository
    keeps its content -- the default graph, named graphs, or both --
    which the Engine establishes before exploration begins.

    See `nile.sphinx.adapters` for the contract the adapters share.
    """

    def __init__(self, endpoint, engine):
        """Bind the adapter to one endpoint.

        Args:
            endpoint: URL of the SPARQL endpoint to explore.
            engine: The `nile.sphinx.sphinx.Engine` that builds and runs
                the exploration queries on this adapter's behalf.
        """
        self.endpoint = endpoint
        self.engine = engine

    def exploratory_types(self):
        """List the classes the repository holds.

        Blank-node classes are left out. A blank node has no identity
        outside the document it came from, so it could never be matched
        against a query's typed variables later on.

        Returns:
            Class IRIs as plain strings, in no particular order. Empty
            if the endpoint holds no typed data or could not be reached.
        """

        results = self.engine.query_endpoint(self.endpoint, "exploratory")

        # Only accept classes bound as a URI/IRI term. A blank-node class
        # (binding["type"] == "bnode") is excluded because it has no
        # stable, cross-repository identity to match against a query's
        # typed variables -- see add_triple_pattern in indexer.py.
        return list({
            r["type"]["value"]
            for r in results
            if r.get("type") and r["type"].get("type") == "uri"
        })

    def outgoing_patterns(self, type_):
        """List the predicates leading out of instances of `type_`.

        Args:
            type_: IRI of the class to explore.

        Returns:
            Solution dicts carrying "predicate" and "object_type", the
            latter absent wherever the object has no explicit class.
            Empty if the endpoint could not be reached.
        """
        return self.engine.query_endpoint(self.endpoint, "fixed_subject", type_)

    def incoming_patterns(self, type_):
        """List the predicates leading into instances of `type_`.

        Args:
            type_: IRI of the class to explore.

        Returns:
            Solution dicts carrying "predicate" and "subject_type", the
            latter absent wherever the subject has no explicit class.
            Empty if the endpoint could not be reached.
        """
        return self.engine.query_endpoint(self.endpoint, "fixed_object", type_)