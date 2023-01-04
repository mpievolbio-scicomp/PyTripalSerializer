"""Console script for tripser."""

import click


@click.command()
def main():
    """Main entrypoint."""
    click.echo("pytripalserializer")
    click.echo("=" * len("pytripalserializer"))
    click.echo("Serialize Tripal's JSON-LD API into RDF.")


if __name__ == "__main__":
    main()  # pragma: no cover
