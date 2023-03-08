#! /usr/bin/env python3

from dask_mpi import initialize
initialize()

from distributed import Client

import time
import sys

from rdflib import Graph

from tripser.tripser import *

client = Client()

parser = RecursiveJSONLDParser('http://pflu.evolbio.mpg.de/web-services/content/v0.1/CDS/11950',
                      graph=Graph(),
                      client=client) # Does this work?

print(parser.client.scheduler_info())
parser.parse()

cleanup(parser.graph)

parser.graph.serialize("pflu_slurm.ttl")




