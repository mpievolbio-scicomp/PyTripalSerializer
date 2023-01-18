#!/usr/bin/env python
"""Tests for `tripser` package."""


import logging
import os
import shutil
import unittest

from rdflib import Graph, URIRef, BNode

from tripser.tripser import RecursiveJSONLDParser
from tripser.tripser import cleanup, get_graph, remove_terms

logging.basicConfig(level=logging.INFO)


class TestRecursiveJSONLDParser(unittest.TestCase):
    def setUp(self):
        """Set up the test case."""

        self._test_data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'test_data'))
        self.__thrashcan = []

    def tearDown(self):
        """Tear down the test case."""

        # Delete all items in the thrashcan.
        for item in self.__thrashcan:
            if os.path.isfile(item):
                os.remove(item)
            elif os.path.isdir(item):
                shutil.rmtree(item)

    def test_get_graph_dbxref(self):
        """Test parsing a URL for dbxref's into a graph."""

        page = "http://pflu.evolbio.mpg.de/web-services/content/v0.1/CDS/11845/database%20cross%20reference"

        graph = get_graph(page)

        self.assertIsInstance(graph, Graph)

        # There should be 125 terms in this graph.
        self.assertEqual(len(graph), 125)

    def test_get_graph(self):
        """Test parsing a URL into a graph."""

        page = "http://pflu.evolbio.mpg.de/web-services/content/v0.1/CDS/11845"

        graph = get_graph(page)

        self.assertIsInstance(graph, Graph)

        # There should be 40 terms in this graph.
        self.assertEqual(len(graph), 40)

    def test_recursively_add_cds(self):
        """Test adding a CDS with all subclasses."""

        cds_page = "http://pflu.evolbio.mpg.de/web-services/content/v0.1/CDS/11845"

        parser = RecursiveJSONLDParser(cds_page)
        parser.parse()

        self.assertIsInstance(parser.graph, Graph)

        # Can only check non-BNode terms
        self.assertEqual(
            len([(s, p, o) for (s, p, o) in parser.graph if not (isinstance(s, BNode) or isinstance(o, BNode))]), 247
        )

    def test_parse_page_cds(self):
        """Test parsing a CDS with all subclasses."""

        cds_page = "http://pflu.evolbio.mpg.de/web-services/content/v0.1/CDS?page=1&limit=1"

        parser = RecursiveJSONLDParser(cds_page)

        cds_graph = parser.parse_page(cds_page)

        self.assertIsInstance(cds_graph, Graph)
        self.assertEqual(
            len([(s, p, o) for (s, p, o) in cds_graph if not (isinstance(s, BNode) or isinstance(o, BNode))]), 253
        )

    @unittest.skip("Takes too long.")
    def test_parse_page(self):
        """Test parsing a URL with members."""

        cds_page = "http://pflu.evolbio.mpg.de/web-services/content/v0.1/CDS?page=3&limit=10"

        parser = RecursiveJSONLDParser(cds_page)

        cds_graph = parser.parse_page(cds_page)

        self.assertIsInstance(cds_graph, Graph)

        # Get number of unique CDS subjects, should be 10.
        self.assertEqual(
            len(
                [
                    tr
                    for tr in cds_graph.triples(
                        (
                            None,
                            URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                            URIRef('http://www.sequenceontology.org/browser/current_svn/term/SO:0000316'),
                        )
                    )
                ]
            ),
            10,
        )

    def test_recursively_add_feature(self):
        """Test recursively adding terms to a graph (no members)."""

        parser = RecursiveJSONLDParser('http://pflu.evolbio.mpg.de/web-services/content/v0.1/CDS/11846')
        g = parser.recursively_add(Graph(), ref=URIRef(parser.entry_point))

        self.assertIsInstance(g, Graph)

        self.assertEqual(len([(s, p, o) for (s, p, o) in g if not (isinstance(s, BNode) or isinstance(o, BNode))]), 248)

    @unittest.skip("Takes too long.")
    def test_recursively_add_trna(self):
        """Test recursively adding terms to a graph (with members)."""
        parser = RecursiveJSONLDParser('http://pflu.evolbio.mpg.de/web-services/content/v0.1/TRNA/')
        g = parser.recursively_add(Graph(), ref=parser.entry_point)

        logging.debug("# Terms")
        for term in g:
            logging.debug("\t %s", str(term))

        # Get number of unique TRNAs subjects, should be 1.
        self.assertEqual(
            len(
                [
                    tr
                    for tr in g.triples(
                        (
                            None,
                            URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                            URIRef('http://www.sequenceontology.org/browser/current_svn/term/SO:0000253'),
                        )
                    )
                ]
            ),
            66,
        )

    def test_recursively_add_tmrna(self):
        """Test recursively adding terms to a graph (with members)."""

        parser = RecursiveJSONLDParser(URIRef('http://pflu.evolbio.mpg.de/web-services/content/v0.1/TMRNA'))
        g = parser.recursively_add(Graph(), ref=parser.entry_point)

        logging.debug("# Terms")
        for term in g:
            logging.debug("\t %s", str(term))

        self.assertEqual(len([(s, p, o) for (s, p, o) in g if not (isinstance(s, BNode) or isinstance(o, BNode))]), 92)

        # Get number of unique TMRNAs subjects, should be 1.
        self.assertEqual(
            len(
                [
                    tr
                    for tr in g.triples(
                        (
                            None,
                            URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                            URIRef('http://www.sequenceontology.org/browser/current_svn/term/SO:0000584'),
                        )
                    )
                ]
            ),
            1,
        )

    def test_construct_and_parse(self):
        """Test instantiating and using the class."""

        # Construct the parser.
        parser = RecursiveJSONLDParser(URIRef('http://pflu.evolbio.mpg.de/web-services/content/v0.1/TMRNA'))

        # Parse.
        parser.parse()

        # Get the graph.
        g = parser.graph

        # Clean it up.
        cleanup(g)

        self.assertGreater(len(g), 0)

    def test_graph_len_consistent(self):
        """Test that the length of the queried graph is always the same in consequtive parsings"""

        parser_1 = RecursiveJSONLDParser(URIRef('http://pflu.evolbio.mpg.de/web-services/content/v0.1/TMRNA'))
        parser_1.parse()
        g_1 = parser_1.graph
        cleanup(g_1)

        parser_2 = RecursiveJSONLDParser(URIRef('http://pflu.evolbio.mpg.de/web-services/content/v0.1/TMRNA'))
        parser_2.parse()
        g_2 = parser_2.graph
        cleanup(g_2)

        self.assertEqual(len(g_1), len(g_2))

    def test_get_graph_corrupt_json(self):
        """Test get_graph() for a corrupt json file."""

        corrupt_json_url = (
            "https://github.com/mpievolbio-scicomp/PyTripalSerializer/raw/main/tests/test_data/corrupt.json"
        )
        g = get_graph(corrupt_json_url)

        self.assertIsInstance(g, Graph)
        self.assertEqual(len(g), 0)

    def test_get_graph_no_members(self):
        """Test get_graph() with a document that has an empty members list."""

        page = "http://pflu.evolbio.mpg.de/web-services/content/v0.1/Biological_Region?page=781&limit=25"

        g = get_graph(page)

        self.assertIsInstance(g, Graph)
        self.assertEqual(len(g), 5)

    def test_parse_page_no_members(self):
        page = "http://pflu.evolbio.mpg.de/web-services/content/v0.1/Biological_Region?page=781&limit=25"

        parser = RecursiveJSONLDParser(page)

        g = parser.parse_page(page)

        self.assertIsInstance(g, Graph)
        self.assertEqual(len(g), 5)

    def test_remove_terms(self):
        """Test removing multiple terms from a graph."""

        graph = Graph().parse(os.path.join(self._test_data_dir, 'trna_messy.ttl'))

        self.assertEqual(len(graph), 1732)

        remove_terms(
            graph,
            (
                None,
                URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                URIRef('http://www.sequenceontology.org/browser/current_svn/term/SO:0000253'),
            ),
        )

        self.assertEqual(len(graph), 1666)

    def test_cleanup(self):
        """Test cleaning up a graph."""

        logging.getLogger().setLevel(logging.DEBUG)

        messy = Graph().parse(os.path.join(self._test_data_dir, 'trna_messy.ttl'))

        self.assertEqual(len(messy), 1732)
        cleanup(messy)

        self.assertEqual(len(messy), 1725)


if __name__ == "__main__":
    unittest.main()
