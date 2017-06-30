import click
import ega
import urllib
import logging
import logging.config

@click.group()
@click.option('--auth', '-a', default='', help='Login credentials', envvar='EGASUB_AUTH')
@click.option('--force', '-f', default=False, is_flag=True, help='Force overwrite or resubmit', envvar='EGASUB_FORCE')
@click.option('--debug/--no-debug', '-d', default=False, envvar='EGASUB_DEBUG')
@click.pass_context
def cli(ctx, auth, force, debug):
    # initializing ctx.obj
    ctx.obj = {}
    ctx.obj['AUTH'] = urllib.quote(auth, safe='')
    ctx.obj['FORCE'] = force
    ctx.obj['DEBUG'] = debug
    ctx.obj['IS_TEST'] = False
    if ctx.obj['DEBUG']: click.echo('Debug is on.', err=True)

    ega.initialize(ctx)

@cli.command()
@click.argument('task', nargs=1)
@click.pass_context
def audit(ctx, task):
	
	pass


@cli.command()
@click.pass_context
def job():
	pass



if __name__ == '__main__':
	utils.setup_logging()
    cli()