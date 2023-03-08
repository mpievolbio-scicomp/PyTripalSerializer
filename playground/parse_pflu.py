#! /usr/bin/env python3

from rdflib import Graph

from tripser.tripser import RecursiveJSONLDParser, recursively_add, cleanup

parser = RecursiveJSONLDParser('http://pflu.evolbio.mpg.de/web-services/content/v0.1/', 
                               graph=Graph(),
                               client='127.0.0.1:8786'
                               )

parser.client.restart()

parser.parse()

cleanup(parser.graph)

parser.graph.serialize("pflu-20230308.ttl")


