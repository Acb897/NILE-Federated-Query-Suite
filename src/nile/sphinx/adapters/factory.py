"""Choosing the source adapter that matches a repository."""

from nile.sphinx.adapters.sparql_adapter import SPARQLAdapter
from nile.sphinx.adapters.dump_adapter import RDFDumpAdapter
from nile.sphinx.adapters.tpf_adapter import TPFAdapter

# adapters/factory.py

class AdapterFactory:
    """Builds the adapter for a repository's publication mechanism.

    The class is a namespace for `create`. It holds no state and is
    never instantiated.
    """

    @staticmethod
    def create(source, mode="sparql", engine=None): 
        """Build an adapter for one repository.

        Args:
            source: Where the repository lives -- a SPARQL endpoint URL
                in "sparql" mode, a TPF/QPF server URL in "tpf" mode, or
                a path to an RDF file in "dump" mode.
            mode: Which mechanism `source` exposes: "sparql", "dump" or
                "tpf".
            engine: The `nile.sphinx.sphinx.Engine` running the
                exploration. Required in "sparql" mode, which hands
                query building and execution back to it; ignored
                otherwise.

        Returns:
            An adapter exposing `exploratory_types`,
            `outgoing_patterns` and `incoming_patterns`. See
            `nile.sphinx.adapters` for the shared contract.

        Raises:
            ValueError: If `mode` is not one of the three known modes,
                or if "sparql" mode was requested without an engine.
        """
        if mode == "sparql":
            if engine is None:
                raise ValueError("SPARQLAdapter requires engine instance")
            return SPARQLAdapter(source, engine)  
        elif mode == "dump":
            return RDFDumpAdapter(source)
        elif mode == "tpf":
            return TPFAdapter(source)
        else:
            raise ValueError(f"Unknown mode: {mode}")