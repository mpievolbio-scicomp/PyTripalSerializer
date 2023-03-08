#! /usr/bin/env python3

from dask_jobqueue import SLURMCluster
from distributed import Client

import time
import sys

from rdflib import Graph

from tripser.tripser import *


cluster = SLURMCluster(memory="8g",
        queue='highmem',
        walltime='00:03:00',
        cores=48,
        job_extra_directives=['--nodes 2', '-n 96', '--cpus-per-task=1'],
        n_workers=96
        )

cluster.scale(n=96, jobs=1, )

print(cluster.job_script())
sys.exit()

time.sleep(30)




parser = RecursiveJSONLDParser('http://pflu.evolbio.mpg.de/web-services/content/v0.1/',
                      graph=Graph(),
                      client=cluster)

print(parser.client.scheduler_info())
parser.parse()

cleanup(parser.graph)

parser.graph.serialize("pflu_slurm.ttl")




