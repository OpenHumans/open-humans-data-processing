import click

from .__init__ import PGPSource

cli = PGPSource.make_cli()
cli = click.option('--hu-id')(cli)

cli()
