""":module: tripser - main module."""


import json
import logging
import math
import copy

import urllib
import requests
from rdflib import Graph, URIRef, Namespace

logger = logging.getLogger(__name__)


class RecursiveJSONLDParser:
    """:class: This class implements recursive parsing of JSON-LD documents."""

    def __init__(self, entry_point=None, graph=None, serialize_nodes=False):
        """Initialize the recursine JSON-LD parser.

        :param root: The entry point for parsing.
        :type  root: str | rdflib.URIRef | rdflib.Literal

        :param graph: Set the initial graph. Parsed terms will be inserted into this graph.
        :type  graph: rdflib.Graph

        :param serialize_nodes: Determines whether parsed documents will be serialized
        as standalone nodes. Default is False.
        :type  serialize_nodes: bool

        """

        # Init hidden attributes.
        self.__parsed_pages = []
        self.__tasks = []

        # Init public attributes.
        self.graph = graph
        self.serialize_nodes = serialize_nodes
        self.entry_point = entry_point

    @property
    def serialize_nodes(self):
        """Get the 'serialize_nodes' flag."""
        return self.__serialize_nodes

    @serialize_nodes.setter
    def serialize_nodes(self, value):
        """Set the 'serialize_nodes' flag."""
        if value is None:
            value = False
        if not isinstance(value, bool):
            raise TypeError("Parameter 'serialize_nodes' must be a bool, got {}.".format(type(value)))
        self.__serialize_nodes = value

    @property
    def graph(self):
        """Access the graph of the parser."""
        return self.__graph

    @graph.setter
    def graph(self, value):
        """Set the graph attribute"""
        if value is None:
            value = Graph(bind_namespaces='rdflib')
        if not isinstance(value, Graph):
            raise TypeError("Expected instance of rdflib.Graph, received {}".format(type(value)))
        else:
            # Copy construct to leave passed graph as is.
            self.__graph = copy.deepcopy(value)

        self.__graph.bind('ssr', Namespace("http://semanticscience.org/resource/"))
        self.__graph.bind('edam', Namespace("http://edamontology.org/"))
        self.__graph.bind('obo', Namespace("http://purl.obolibrary.org/obo/"))
        self.__graph.bind('so', Namespace("http://www.sequenceontology.org/browser/current_svn/term/SO:"))
        self.__graph.bind('hydra', Namespace("http://www.w3.org/ns/hydra/core#"))
        self.__graph.bind('ncbitax', Namespace("https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id="))
        self.__graph.bind(
            "pflutranscript", Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Transcript/")
        )
        self.__graph.bind("pflu", Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/"))
        self.__graph.bind("pflucv", Namespace("http://pflu.evolbio.mpg.de/cv/lookup/local/"))
        self.__graph.bind("pflucds", Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/CDS/"))
        self.__graph.bind("pflumrna", Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/mRNA/"))
        self.__graph.bind("pflugene", Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Gene/"))
        self.__graph.bind("pfluexon", Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Exon/"))
        self.__graph.bind("pfluorganism", Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Organism/"))

    @property
    def parsed_pages(self):
        return self.__parsed_pages

    @parsed_pages.setter
    def parsed_pages(self, value):
        raise RuntimeError("parsed_pages is a read-only property.")

    @property
    def entry_point(self):
        return self.__entry_point

    @entry_point.setter
    def entry_point(self, value):

        if value is None:
            logging.warning("No entry point set. Set the entry point before parsing.")
            self.__entry_point = None
            return

        if isinstance(value, str):
            value = URIRef(value)

        if not isinstance(value, URIRef):
            raise TypeError("{} is neither a rdflib.URIRef instance nor str.".format(value))
        else:
            self.__entry_point = value
            self.__tasks.append(value)

    def parse(self):

        while True:
            if len(self.__tasks) == 0:
                return

            task = self.__tasks.pop(0)

            # Add items.
            self.recursively_add(task)

    def recursively_add(self, task):
        """
        Parse the document in `ref` into the graph `g`. Then call this function on all 'member' objects of the
        subgraph with the same graph `g`. Serial implementation

        :param g: The graph into which all terms are to be inserted.
        :type  g: rdflib.Graph

        :param ref: The URL of the document to (recursively) parse into the graph
        :type  ref: URIRef | str
        """
        gloc = get_graph(task)

        for term in gloc:
            logger.debug("\t %s", str(term))
            subj, pred, obj = term
            if str(obj) in self.__parsed_pages:
                logger.debug("Already parsed or parsing: %s", str(obj))
                continue
            if pred == URIRef("http://www.w3.org/ns/hydra/core#PartialCollectionView"):
                continue
            if pred == URIRef("http://www.w3.org/ns/hydra/core#totalItems"):
                continue
            if obj.startswith("http://pflu.evolbio.mpg.de/web-services/content/"):
                self.__tasks.append(str(obj))

        if task not in self.__parsed_pages:
            self.graph += gloc
            self.__parsed_pages.append(task)

        # Get total item count.
        members = [ti for ti in gloc.objects(predicate=URIRef("http://www.w3.org/ns/hydra/core#totalItems"))]

        logging.debug("Number of members: %d", len(members))

        # If there are any member, parse them recursively.
        if members != []:
            # Convert to python type.
            nom = members[0].toPython()
            if nom == 0:
                return

            logger.info("Found %d members in %s.", nom, gloc)

            # We'll apply pagination with 25 items per page.
            limit = 25
            pages = range(1, math.ceil(nom / limit) + 1)

            # Get each page's URL.
            pages = [task + "?limit={}&page={}".format(limit, page) for page in pages]
            logger.debug("# PAGES")
            for page in pages:
                logger.debug("\t %s", page)
                self.__tasks.append(page)

def cleanup(grph):
    """
    Remove:
    - All subjects of type  <http://pflu.evolbio.mpg.de/web-services/content/v0.1/PartialCollectionView>
    - All objects with property <hydra:PartialCollectionView>

    :param grph: The graph to cleanup
    """

    remove_terms(grph, (None, None, URIRef('file:///tmp/PartialCollectionView')))

    remove_terms(
        grph, (None, None, URIRef('http://pflu.evolbio.mpg.de/web-services/content/v0.1/PartialCollectionView'))
    )

    remove_terms(grph, (None, URIRef('http://www.w3.org/ns/hydra/core#PartialCollectionView'), None))


def remove_terms(grph, terms):
    """
    Remove terms matching the passed pattern `terms` from `grph`.

    :param grph: The graph from which to remove terms.
    :type  grph: rdflib.Graph

    :param terms: Triple pattern to match against.
    :type  terms: 3-tuple (subject, predicate, object)

    """

    count = 0
    for t in grph.triples(terms):
        grph.remove(t)
        count += 1

    logger.debug("Removed %d terms matching triple pattern (%s, %s, %s).", count, *terms)


def get_graph(page, serialize=False):
    """Workhorse function to download the json document and parse into the graph to be returned.

    :param page: The URL of the json-ld document to download and parse.
    :type  page: str | URIRef

    :return: A graph containing all terms found in the downloaded json-ld document.
    :rtype: rdflib.Graph

    """

    logger.debug("get_graph(graph=%s)", str(page))
    logger.debug("Setting up empty graph.")
    grph = Graph()

    logger.debug("Attempting to parse %s", page)
    try:
        response = requests.get(page, timeout=600)
        jsn = json.dumps(response.json())
        jsn = jsn.replace('https://pflu', 'http://pflu')
        jsn = urllib.parse.unquote(jsn)

        grph.parse(data=jsn, format='json-ld')

    except json.decoder.JSONDecodeError:
        logger.warning("Not a valid JSON document: %s", page)

    except requests.exceptions.JSONDecodeError:
        logger.warning("Not a valid JSON document: %s", page)

    except Exception as e:
        logger.error("Exception thrown while parsing %s.", page)
        raise e

    logger.debug("Parsed %d terms.", len(grph))

    if serialize:
        ofname = page.split("://")[-1].replace("/", "__") + ".ttl"
        logger.info("Writing %s to %s.", page, ofname)
        grph.serialize(ofname)

    return grph
