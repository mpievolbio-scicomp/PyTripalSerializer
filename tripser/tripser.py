""":module: tripser - main module."""


import json
import logging
import math
import multiprocessing
import tempfile

import requests
from rdflib import Graph, URIRef

logging.basicConfig(level=logging.WARNING)


def parse_page(page):
    """
    This function will attempt to get the json-ld blob from the passed page (URL) and pass it on to Graph.parse().
    It then calls the `recursively_add` function on the local scope's graph and for each member's URI.

    The constructed Graph instance is returned.

    :param page: URL of the json-ld document
    :type  page: str

    :return: A Graph instance constructed from the downloaded json document.
    :rtype: Graph
    """

    logging.debug("Getting %s", page)
    grph = get_graph(page)

    for o in grph.objects(predicate=URIRef('http://www.w3.org/ns/hydra/core#member'), unique=True):
        grph = recursively_add(grph, o)

    return grph


def get_graph(page):
    """Workhorse function to download the json document and parse into the graph to be returned.

    :param page: The URL of the json-ld document to download and parse.
    :type  page: str | URIRef

    :return: A graph containing all terms found in the downloaded json-ld document.
    :rtype: rdflib.Graph

    """

    with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as fh:
        logging.debug(fh.name)

        response = requests.get(page)
        content = response.content

        logging.debug(response.text)

        fh.write(content)

    grph = Graph()

    try:
        grph.parse(fh.name)

    except json.decoder.JSONDecodeError:
        logging.warning("Not a valid JSON document: %s", page)

    except BaseException:
        logging.error("Exception thrown while parsing %s.", page)
        raise

    return grph


def recursively_add(g, ref, number_of_processes=multiprocessing.cpu_count() // 2):
    """
    Parse the document in `ref` into the graph `g`. Then call this function on all 'member' objects of the
    subgraph with the same graph `g`.

    :param g: The graph into which all terms are to be inserted.
    :type  g: rdflib.Graph

    :param ref: The URL of the document to (recursively) parse into the graph
    :type  ref: URIRef | str
    """

    # First parse the document into a local g.
    # gloc = get_graph(ref)
    gloc = Graph().parse(ref)

    # Get total item count.
    number_of_members = [ti for ti in gloc.objects(predicate=URIRef("http://www.w3.org/ns/hydra/core#totalItems"))]

    # If there are any member, parse them recursively.
    if number_of_members != []:
        # Convert to python type.
        nom = number_of_members[0].toPython()
        if nom == 0:
            return g + gloc

        logging.info("Found %d members in %s.", nom, ref.toPython())

        # We'll apply pagination with 25 items per page.
        limit = 25
        pages = range(1, math.ceil(nom / limit) + 1)

        # Get each page's URL.
        pages = [ref + "?limit={}&page={}".format(limit, page) for page in pages]

        # Get pool of workers and  distribute tasks.
        number_of_tasks = len(pages)
        number_of_processes = min(multiprocessing.cpu_count(), number_of_tasks)
        chunk_size = number_of_tasks // number_of_processes

        logging.info("### MultiProcessing setup")
        logging.info("### Number of tasks:\t\t%d", number_of_tasks)
        logging.info("### Number of processes:\t%d", number_of_processes)
        logging.info("### Chunk size:\t\t%d", chunk_size)

        with multiprocessing.Pool(processes=number_of_processes) as pool:
            logging.debug("Setup pool %s.", str(pool))
            list_of_graphs = pool.map_async(parse_page, pages, chunksize=chunk_size).get()

            pool.close()
            pool.join()

        logging.info("Done parsing subgraphs in %s.", ref)

        logging.info("Merging subgraphs in %s.", ref)
        for grph in list_of_graphs:
            gloc = gloc + grph

    return g + gloc


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

    logging.debug("Removed %d terms matching triple pattern (%s, %s, %s).", count, *terms)
