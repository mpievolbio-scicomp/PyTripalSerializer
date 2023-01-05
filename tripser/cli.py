#!/usr/bin/env python3
"""Console script for tripser."""

import logging

import click
from rdflib import Graph

from tripser import tripser

logger = logging.getLogger("tripser.cli")
logger.setLevel(logging.INFO)


@click.command()
@click.argument('url')
@click.option('-o', '--out', default='graph.ttl', help='')
def cli(url, out):
    """Main entrypoint."""
    click.echo("pytripalserializer")
    click.echo("=" * len("pytripalserializer"))
    click.echo("Serialize Tripal's JSON-LD API into RDF.")

    try:
        g = tripser.recursively_add(Graph(), url)
        tripser.cleanup(g)
        g.serialize(out)
        logger.info(
            "Successfully parsed %s. After cleanup, %d triples remain and will be written to %s", url, len(g), out
        )
    except Exception:
        logger.error("Could not parse '%s', please check the URL.", url)


if __name__ == "__main__":
    cli()  # pragma: no cover
