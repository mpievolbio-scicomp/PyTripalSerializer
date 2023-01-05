#!/usr/bin/env python
""":module: test_cli testing module."""

import os
import shutil
import unittest

from click.testing import CliRunner
from rdflib import Graph

from tripser.cli import cli


class CLITest(unittest.TestCase):
    def setUp(self):
        """Set up the test case."""

        self.runner = CliRunner()

        self._thrashcan = []

    def tearDown(self):
        """Tear down the test case."""

        # Remove thrash
        for item in self._thrashcan:
            if os.path.isfile(item):
                os.remove(item)
            elif os.path.isdir(item):
                shutil.rmtree(item)
            else:
                pass

    def test_defaults(self):
        """Test the CLI with no parameters envoked."""

        response = self.runner.invoke(cli)

        self.assertEqual(response.exit_code, 2)

    def test_cds_11846_default_output(self):
        """Test parsing a json document and load as graph from default output file."""

        self._thrashcan.append('graph.ttl')

        response = self.runner.invoke(cli, ['http://pflu.evolbio.mpg.de/web-services/content/v0.1/CDS/11846'])
        g = Graph().parse('graph.ttl')

        self.assertEqual(response.exit_code, 0)
        self.assertEqual(len(g), 42)

    def test_cds_11846_nondefault_output(self):
        """Test parsing a json document and load as graph from specified output file."""

        self._thrashcan.append('11846.ttl')

        response = self.runner.invoke(
            cli, '--out 11846.ttl http://pflu.evolbio.mpg.de/web-services/content/v0.1/CDS/11846'
        )
        g = Graph().parse('11846.ttl')

        self.assertEqual(response.exit_code, 0)
        self.assertEqual(len(g), 42)


if __name__ == "__main__":
    unittest.main()
