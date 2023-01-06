# PyTripalSerializer
[![Documentation Status](https://readthedocs.org/projects/pytripalserializer/badge/?version=latest)](https://pytripalserializer.readthedocs.io/en/latest/?badge=latest)
[![build & test](https://github.com/mpievolbio-scicomp/PyTripalSerializer/actions/workflows/dev.yml/badge.svg)](https://github.com/mpievolbio-scicomp/PyTripalSerializer/actions/workflows/dev.yml)
## Serialize Tripal's JSON-LD API into RDF format
This package implements a recursive algorithm to parse the JSON-LD API of a [Tripal](https://tripal.info "Tripal")
genomic database webservice and serialize the encountered terms into a RDF document. Output will be saved in
a turtle file (.ttl).

## Motivation
This work is a byproduct of a data integration project for multiomics data at [MPI for Evolutionary Biology](https://evolbio.mpg.de). Among various other data sources, we run an instance of the Tripal genomic database website engine. This
service provides a [JSON-LD](https://json-ld.org/) API, i.e., all data in the underlying relational database is accessible through appropriate http GET requests against that API. So far so good. Now, in our project, we are working
on integrating data based on Linked Data technology; in particular, all data sources should be accessible via (federated) SPARQL queries. Hence, the task is to convert the JSON-LD API into a SPARQL endpoint.

The challenge here is that the JSON-LD API only provides one document at a time. Querying a single document with e.g.
the `arq` utility (part of the [Apache-Jena](https://jena.apache.org/) package) is no problem. The problem starts
when one then attempts to run queries against other JSON-LD documents referenced in the first document as object URIs but. These object URIs are not part of the current document (graph). Instead, they point to separate graph.
SPARQL in its current implementation does not support dynamic generation of graph URIs from e.g. object URIs.
Hence the need for a code that recursively parses a JSON-LD document including all referenced documents.

Of course this is a generic problem. This package implements a solution targeted for Tripal JSON-LD APIs but with minimal changes it should be adaptable for other JSON-LD APIs.
## Installation

### PyPI Releases
This package is released via the Python Package Index (PyPI). To install it, run

```console
$ pip install pytripalserializer
```

### Github development snapshot
To install the latest development snapshot from github, clone this repository

```console
git clone https://github.com/mpievolbio-scicomp/PyTripalSerializer
```

Navigate into the cloned directory and run a local `pip install`:

```console
cd PyTripalSerializer
pip install [-e] .
```
The optional flag `-e` would instruct `pip` to install symlinks to the source files, this is recommended for developers.

## Usage
The simplest way to use the package is via the command line interface. The following example should
be instructive enough to get started:

```console
$ cd PyTripalSerializer
$ cd src
$ ./tripser http://pflu.evolbio.mpg.de/web-services/content/v0.1/CDS/11846 -o cds11846.ttl
```

Running this command should produce the RDF turtle file "cds11846.ttl" in the src/ directory. "cds11846" has only 42 triples.

Be aware that running the command on a top level URL such as http://pflu.evolbio.mpg.de/web-services/content/v0.1/ would parse the entire tree of documents which results in a graph of ~2 million triples and takes roughly 14hrs to complete on a reasonably well equipped workstation with 48 CPUs.

## Testing
Run the test suite with

```console
pytest tests
```

## Documentation
Click the documentation badge at the top of this README to access the online manual.
