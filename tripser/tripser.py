""":module: tripser - main module."""


import copy
import json
import math
import urllib
import time

import requests
from rdflib import Graph, Namespace, URIRef


from dask.distributed import Client
from dask.distributed.scheduler import logger


class RecursiveJSONLDParser:
    """:class: This class implements recursive parsing of JSON-LD documents."""

    def __init__(self, entry_point=None, graph=None, serialize_nodes=False, client=None, scheduler='127.0.0.1:8786'):
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

        if client is not None:
            if isinstance(client, Client):
                self.client = client
            else:
                raise TypeError("Parameter 'client' must be of type dask.distributed.Client, found {}.".format(type(client)))
        elif scheduler is not None:
            try:
                self.client = Client(scheduler)
            except:
                raise TypeError('Must pass a valid scheduler URL, typically 127.0.0.1:8786.')
        else:
            raise AttributeError("Pass either client or scheduler.")


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
            logger.warning("No entry point set. Set the entry point before parsing.")
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
        """ Start the parsing loop. In each loop, the first task is processed (parsed) and
        resulting tasks from object URIs are appended to the task queue.
        """

        # Number of complete tasks.
        no_complete = 0

        # Start loop

        parsed_pages = self.__parsed_pages
        graph = self.__graph
        tasks = self.__tasks

        while len(tasks) > 0:
            # Add items.
            no_tasks = len(tasks)

            # Process in chunks to avoid DoS breakdown.
            chunk_length = 8
            task_chunks = [tasks[chunk_length*s:chunk_length*(s+1)] for s in range(len(tasks)//chunk_length)]
            results = []
            for chunk in task_chunks:
                results += [self.client.submit(recursively_add, task) for task in chunk]
                time.sleep(10)

                # Move scheduled tasks to parsed_pages for bookkeeping.
                parsed_pages += [t for t in chunk]

            # Empty task list.
            tasks = []
            for result in self.client.gather(results):
                tasks += result[0]
                graph += result[1]

            no_complete += no_tasks

            tasks = list(set(tasks).difference(set(parsed_pages)))

            # Report if multiple of 100 tasks complete.
            if no_complete % 100 == 0:
                logger.info("parsed %d pages,\
                            %d tasks complete,\
                            %d tasks to do,\
                            graph has %d terms.",
                            len(parsed_pages),
                            no_complete,
                            no_tasks - 1,
                            len(graph)
                            )
            # No need to update self.graph since local variable graph is a reference on self.graph.


def recursively_add(task):
    """
    Parse the document in `ref` into the graph `g`. Then call this function on all 'member' objects of the
    subgraph with the same graph `g`. Serial implementation

    :param g: The graph into which all terms are to be inserted.
    :type  g: rdflib.Graph

    :param ref: The URL of the document to (recursively) parse into the graph
    :type  ref: URIRef | str
    """
    gloc = get_graph(task)

    collected_tasks = []

    for term in gloc:
        logger.debug("\t %s", str(term))
        subj, pred, obj = term
        if pred == URIRef("http://www.w3.org/ns/hydra/core#PartialCollectionView"):
            continue
        if pred == URIRef("http://www.w3.org/ns/hydra/core#totalItems"):
            continue
        if obj.startswith("http://pflu.evolbio.mpg.de/web-services/content/"):
            collected_tasks.append(str(obj))

    # Only if we are not paginating yet
    if len(task.split("&")) > 1:
        return collected_tasks, gloc

    # Get total item count.
    members = [ti for ti in gloc.objects(predicate=URIRef("http://www.w3.org/ns/hydra/core#totalItems"))]

    logger.debug("Number of members: %d", len(members))

    # If there are any member, parse them recursively.
    if members != []:
        # Convert to python type.
        nom = members[0].toPython()
        if nom == 0:
            return collected_tasks, gloc

        logger.debug("Found %d members in %s.", nom, gloc)

        # We'll apply pagination with 25 items per page.
        limit = 25
        pages = range(1, math.ceil(nom / limit) + 1)

        # Get each page's URL.
        collected_tasks += [URIRef(task + "?limit={}&page={}".format(limit, page)) for page in pages]
        logger.debug("# PAGES")

    return collected_tasks, gloc

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

SSR = Namespace("http://semanticscience.org/resource/")
EDAM = Namespace("http://edamontology.org/")
SCHEMA = Namespace("https://schema.org/")
OBO = Namespace("http://purl.obolibrary.org/obo/")
SO = Namespace("http://purl.obolibrary.org/obo/so#")
HYDRA = Namespace("http://www.w3.org/ns/hydra/core#")
NCBITAX = Namespace("https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=")
TRANSCRIPT = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Transcript/")
PFLU = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/")
TRIPAL3 = Namespace("http://pflu.evolbio.mpg.de/cv/lookup/local/")
LOCAL = Namespace("http://pflu.evolbio.mpg.de/cv//local/")
Analysis = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Analysis/")
Binding_Site = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Binding_Site/") 
Biological_Region = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Biological_Region/") 
CDS = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/CDS/")
Exon = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Exon/")
Gene = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Gene/")
Genetic_Map = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Genetic_Map/")
Genetic_Marker = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Genetic_Marker/") 
Genome_Annotation = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Genome_Annotation/") 
Genome_Assembly = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Genome_Assembly/") 
Germplasm_Accession = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Germplasm_Accession/") 
heritable_phenotypic_marker = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Heritable_Phenotypic_Marker/") 
mRNA = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/mRNA/")
ncRNA = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/ncRNA/")
Organism = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Organism/")
Phylogenetic_Tree = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Phylogenetic_Tree/") 
Physical_Map = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Physical_Map/") 
Protein_Binding_Site = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Protein_Binding_Site/") 
Pseudogene = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Pseudogene/")
Pseudogenic_CDS = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Pseudogenic_CDS/") 
Pseudogenic_Exon = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Pseudogenic_Exon/") 
Pseudogenic_Transcript = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Pseudogenic_Transcript/") 
Publication = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Publication/")
QTL = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/QTL/")
Regulatory_Region = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Regulatory_Region/") 
Repeat_Region = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Repeat_Region/") 
rRNA = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/RRNA/")
Sequence_Difference = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Sequence_Difference/") 
Sequence_Variant = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Sequence_Variant/") 
Signal_Peptide = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Signal_Peptide/") 
Stem_Loop = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Stem_Loop/")
tMRNA = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/TmRNA/")
tRNA = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/TRNA/")
Transcript = Namespace("http://pflu.evolbio.mpg.de/web-services/content/v0.1/Transcript/")

def set_bindings(graph):
    """ Bind graph to namespaces """
    graph.bind('ssr', SSR )
    graph.bind('edam', EDAM)
    graph.bind('schema', SCHEMA)
    graph.bind('OBO', OBO)
    graph.bind('so', SO)
    graph.bind("SO", Namespace("http://purl.obolibrary.org/obo/SO_"))
    graph.bind('hydra', HYDRA)
    graph.bind('ncbitax', NCBITAX)
    graph.bind("pflu", PFLU)
    graph.bind("tripal3", TRIPAL3)
    graph.bind("local", LOCAL)
    graph.bind("Analysis", Analysis)
    graph.bind("Binding_Site", Binding_Site)
    graph.bind("Biological_Region", Biological_Region)
    graph.bind("CDS", CDS)
    graph.bind("Exon", Exon)
    graph.bind("Gene", Gene)
    graph.bind("Genetic_Map", Genetic_Map)
    graph.bind("Genetic_Marker", Genetic_Marker)
    graph.bind("Genome_Annotation", Genome_Annotation)
    graph.bind("Genome_Assembly", Genome_Assembly)
    graph.bind("Germplasm_Accession", Germplasm_Accession)
    graph.bind("heritable_phenotypic_marker", heritable_phenotypic_marker)
    graph.bind("mRNA", mRNA)
    graph.bind("ncRNA", ncRNA)
    graph.bind("Organism", Organism)
    graph.bind("Phylogenetic_Tree", Phylogenetic_Tree)
    graph.bind("Physical_Map", Physical_Map)
    graph.bind("Protein_Binding_Site", Protein_Binding_Site)
    graph.bind("Pseudogene", Pseudogene)
    graph.bind("Pseudogenic_CDS", Pseudogenic_CDS)
    graph.bind("Pseudogenic_Exon", Pseudogenic_Exon)
    graph.bind("Pseudogenic_Transcript", Pseudogenic_Transcript)
    graph.bind("Publication", Publication)
    graph.bind("QTL", QTL)
    graph.bind("Regulatory_Region", Regulatory_Region)
    graph.bind("Repeat_Region", Repeat_Region)
    graph.bind("rRNA", rRNA)
    graph.bind("Sequence_Difference", Sequence_Difference)
    graph.bind("Sequence_Variant", Sequence_Variant)
    graph.bind("Signal_Peptide", Signal_Peptide)
    graph.bind("Stem_Loop", Stem_Loop)
    graph.bind("tMRNA", tMRNA)
    graph.bind("tRNA", tRNA)
    graph.bind("Transcript", Transcript)