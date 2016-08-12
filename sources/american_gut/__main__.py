import click

from .__init__ import AmericanGutSource

cli = AmericanGutSource.make_cli()
cli = click.option('--survey-ids')(cli)

cli()
