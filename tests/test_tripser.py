#!/usr/bin/env python
"""Tests for `tripser` package."""


import logging
import os
import shutil
import unittest

from rdflib import Graph, URIRef

from tripser.tripser import cleanup, get_graph, parse_page, recursively_add, remove_terms


class TestTripser(unittest.TestCase):
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

    def test_get_graph(self):
        """Test parsing a URL into a graph."""

        page = "http://pflu.evolbio.mpg.de/web-services/content/v0.1/CDS/11845"

        graph = get_graph(page)

        self.assertIsInstance(graph, Graph)

        # There should be 40 terms in this graph.
        self.assertEqual(len(graph), 40)

    def test_parse_page(self):
        """Test parsing a URL with members."""

        cds_page = "http://pflu.evolbio.mpg.de/web-services/content/v0.1/CDS?page=3&limit=10"

        cds_graph = parse_page(cds_page)

        self.assertIsInstance(cds_graph, Graph)
        self.assertEqual(len(cds_graph), 377)

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
        g = recursively_add(Graph(), ref=URIRef('http://pflu.evolbio.mpg.de/web-services/content/v0.1/CDS/11846'))

        self.assertIsInstance(g, Graph)
        self.assertEqual(len(g), 42)

    def test_recursively_add_class(self):
        """Test recursively adding terms to a graph (with members)."""
        g = recursively_add(Graph(), ref=URIRef('http://pflu.evolbio.mpg.de/web-services/content/v0.1/TRNA'))

        self.assertEqual(len(g), 1732)

        # Get number of unique TRNAs subjects, should be 66.
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

        g = parse_page(page)

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
