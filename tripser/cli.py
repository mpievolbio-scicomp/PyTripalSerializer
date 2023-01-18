#!/usr/bin/env python3
"""Console script for tripser."""

import logging
import click
from tripser import tripser

logging.basicConfig(level=logging.INFO)
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

    parser = tripser.RecursiveJSONLDParser(url)

    try:
        parser.parse()
        g = parser.graph
        tripser.cleanup(g)
        g.serialize(out)
        logger.info(
            "Successfully parsed %s. After cleanup, %d triples remain and will be written to %s", url, len(g), out
        )
    except Exception as e:
        logger.error("Could not parse '%s', please check the URL.", url)
        if logger.level == logging.DEBUG:
            raise e


if __name__ == "__main__":
    cli()  # pragma: no cover
