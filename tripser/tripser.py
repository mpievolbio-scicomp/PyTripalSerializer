""":module: tripser - main module."""


import json
import logging
import math

import urllib
import requests
from rdflib import Graph, URIRef

logger = logging.getLogger(__name__)


class RecursiveJSONLDParser:
    """:class: This class implements recursive parsing of JSON-LD documents."""

    def __init__(self, entry_point=None):
        """Initialize the recursine JSON-LD parser.

        :param root: The entry point for parsing.
        :type  root: str | rdflib.URIRef | rdflib.Literal

        """

        self.__parsed_pages = []

        self.graph = Graph()

        self.entry_point = URIRef(entry_point)

    @property
    def parsed_pages(self):
        return self.__parsed_pages

    @parsed_pages.setter
    def parsed_pages(self, value):
        raise RuntimeError("parsed_pages is a read-only property.")

    @property
    def graph(self):
        return self.__graph

    @graph.setter
    def graph(self, value):
        if isinstance(value, Graph):
            self.__graph = value
        else:
            raise TypeError("{} is not a rdflib.Graph instance.".format(value))

    @property
    def entry_point(self):
        return self.__entry_point

    @entry_point.setter
    def entry_point(self, value):
        if isinstance(value, str):
            value = URIRef(value)

        if not isinstance(value, URIRef):
            raise TypeError("{} is neither a rdflib.URIRef instance nor str.".format(value))
        else:
            self.__entry_point = value

    def parse(self):
        self.graph += self.parse_page(self.entry_point)

    def parse_page(self, page):
        """
        This function will attempt to get the json-ld blob from the passed page (URL) and pass it on to Graph.parse().
        It then calls the `recursively_add` function on the local scope's graph and for each member's URI.

        The constructed Graph instance is returned.

        :param page: URL of the json-ld document
        :type  page: str

        :return: A Graph instance constructed from the downloaded json document.
        :rtype: Graph
        """

        logger.debug("parse_page(page=%s)", str(page))

        grph = get_graph(page)

        logger.debug("# Terms")
        for term in grph:
            logger.debug("\t %s", str(term))
            subj, pred, obj = term
            if str(obj) in self.__parsed_pages:
                logger.debug("Already parsed or parsing: %s", str(obj))
                continue
            if pred == URIRef("http://www.w3.org/ns/hydra/core#PartialCollectionView"):
                continue
            if obj.startswith("http://pflu.evolbio.mpg.de/web-services/content/"):
                self.__parsed_pages.append(str(obj))
                logger.debug("Descending into 'recursively_add()'")
                grph = self.recursively_add(grph, obj)

        return grph

    def recursively_add_serial(self, g, ref):
        """
        Parse the document in `ref` into the graph `g`. Then call this function on all 'member' objects of the
        subgraph with the same graph `g`. Serial implementation

        :param g: The graph into which all terms are to be inserted.
        :type  g: rdflib.Graph

        :param ref: The URL of the document to (recursively) parse into the graph
        :type  ref: URIRef | str
        """
        logger.debug("recursively_add(g=%s, ref=%s)", str(g), str(ref))
        # First parse the document into a local g.
        # gloc = get_graph(ref)
        gloc = self.parse_page(ref)

        # Get total item count.
        number_of_members = [ti for ti in gloc.objects(predicate=URIRef("http://www.w3.org/ns/hydra/core#totalItems"))]

        # If there are any member, parse them recursively.
        if number_of_members != []:
            # Convert to python type.
            nom = number_of_members[0].toPython()
            if nom == 0:
                return g + gloc

            logger.info("Found %d members in %s.", nom, ref.toPython())

            # We'll apply pagination with 25 items per page.
            limit = 25
            pages = range(1, math.ceil(nom / limit) + 1)

            # Get each page's URL.
            pages = [ref + "?limit={}&page={}".format(limit, page) for page in pages]
            logger.debug("# PAGES")
            for page in pages:
                logger.debug("\t %s", page)

            igraphs = (self.parse_page(page) for page in pages)

            logger.info("Parsing and merging subgraphs in %s.", ref)
            for grph in igraphs:
                gloc = gloc + grph

        return g + gloc

    def recursively_add(self, g, ref):
        """
        Parse the document in `ref` into the graph `g`. Then call this function on all 'member' objects of the
        subgraph with the same graph `g`.

        :param g: The graph into which all terms are to be inserted.
        :type  g: rdflib.Graph

        :param ref: The URL of the document to (recursively) parse into the graph
        :type  ref: URIRef | str
        """

        return self.recursively_add_serial(g, ref)


#         # First parse the document into a local g.
#         gloc = get_graph(ref)
#         # gloc = Graph().parse(ref)

#         # Get total item count.
#         number_of_members = [ti for ti in gloc.objects(
#                                 predicate=URIRef("http://www.w3.org/ns/hydra/core#totalItems"))]

#         # If there are any member, parse them recursively.
#         if number_of_members != []:
#             # Convert to python type.
#             nom = number_of_members[0].toPython()
#             if nom == 0:
#                 return g + gloc

#             logger.info("Found %d members in %s.", nom, ref.toPython())

#             # We'll apply pagination with 25 items per page.
#             limit = 25
#             pages = range(1, math.ceil(nom / limit) + 1)

#             # Get each page's URL.
#             pages = [ref + "?limit={}&page={}".format(limit, page) for page in pages]

#             # Get pool of workers and  distribute tasks.
#             number_of_tasks = len(pages)
#             number_of_processes = min(multiprocess.cpu_count(), number_of_tasks)
#             chunk_size = number_of_tasks // number_of_processes

#             logger.info("### MultiProcessing setup")
#             logger.info("### Number of tasks:\t\t%d", number_of_tasks)
#             logger.info("### Number of processes:\t%d", number_of_processes)
#             logger.info("### Chunk size:\t\t%d", chunk_size)

#             with ProcessPool(nodes=number_of_processes) as pool:
#                 logger.debug("Setup pool %s.", str(pool))
#                 list_of_graphs = pool.amap(parse_page, pages, chunksize=chunk_size).get()

#                 pool.close()
#                 pool.join()

#             logger.info("Done parsing subgraphs in %s.", ref)

#             logger.info("Merging subgraphs in %s.", ref)
#             for grph in list_of_graphs:
#                 gloc = gloc + grph

#         return g + gloc


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


def get_graph(page):
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

    except BaseException:
        logger.error("Exception thrown while parsing %s.", page)
        raise

    logger.debug("Parsed %d terms.", len(grph))
    return grph
