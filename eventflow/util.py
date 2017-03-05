import argparse
import warnings
import functools
import configparser
import os

from sshtunnel import SSHTunnelForwarder
from pymongo import MongoClient

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini"))

SSH_HOST = config["SSH"]["SSH_HOST"]
SSH_PORT = int(config["SSH"]["SSH_PORT"])
LDAP_USER_NAME = config["SSH"]["LDAP_USER_NAME"]
LDAP_PASSWORD = config["SSH"]["LDAP_PASSWORD"]
MONGODB_HOST = config["MONGODB"]["MONGODB_HOST"]
MONGODB_PORT = int(config["MONGODB"]["MONGODB_PORT"])
MONGODB_AUTHENTICATION_DB = config["MONGODB"]["MONGODB_AUTHENTICATION_DB"]
MONGODB_USER_NAME = config["MONGODB"]["MONGODB_USER_NAME"]
MONGODB_PASSWORD = config["MONGODB"]["MONGODB_PASSWORD"]


EVENT_TRIPLES = config["DATABASE"]["EVENT_TRIPLES"]
WIKIDATA = config["DATABASE"]["WIKIDATA"]
WIKIDATA_EN = config["DATABASE"]["WIKIDATA_EN"]

EVENTFLOW_COLLECTION = config["DATABASE"]["EVENTFLOW_COLLECTION"]
EVENTFLOW_NODES = config["DATABASE"]["EVENTFLOW_NODES"]
EVENTFLOW_EDGES = config["DATABASE"]["EVENTFLOW_EDGES"]

def adrastea(*args, **kwargs):
    """Wrapper for the automatic ssh connection to the specified SSH-Port and
    MongoDB. The connection details can be set in the config.ini file.

    Possible kwargs:

    :param extra_args: function that accepts a configparser and adds
        parser arguments
    :type extra_args: function(parser): return parser

    Usage is as simple as::

        from eventflow.util import adrastea

        @adrastea()
        def foo():
    """
    def adrastea_inner(func):
        def ssh_connect(*args2, **kwargs2):
            with SSHTunnelForwarder((SSH_HOST, SSH_PORT),
                                    ssh_username=LDAP_USER_NAME,
                                    ssh_password=LDAP_PASSWORD,
                                    remote_bind_address=('localhost', MONGODB_PORT),
                                    local_bind_address=('localhost', MONGODB_PORT)
                                    ) as server:
                print('Connected via SSH and established port-forwarding')
                client = MongoClient(MONGODB_HOST, MONGODB_PORT)
                try:
                    client[MONGODB_AUTHENTICATION_DB].authenticate(
                        MONGODB_USER_NAME, MONGODB_PASSWORD)
                    env = dict()
                    env['client'] = client
                    if "extra_args" in kwargs:
                        parser = argparse.ArgumentParser()
                        env.update(vars(kwargs["extra_args"](parser)))
                    print('Authenticated on mongodb')
                    print('-' * 70)
                    print('')
                    args2 = list(args2)
                    args2.append(env)
                    result = func(*args2, **kwargs2)
                finally:
                    client.close()
                    print('')
                    print('-' * 70)
                    print('Connection closed')

            return result
        return ssh_connect
    return adrastea_inner


def date_parser(series):
    years = series.apply(lambda x: int(x.split("-")[0]))
    months = series.apply(lambda x: int(
        x.split("-")[1]) if len(x.split("-")) > 1 else 0)
    days = series.apply(lambda x: int(
        x.split("-")[2]) if len(x.split("-")) > 2 else 0)
    return years, months, days

try:
    from line_profiler import LineProfiler

    def do_profile(follow=[]):
        def inner(func):
            def profiled_func(*args, **kwargs):
                try:
                    profiler = LineProfiler()
                    profiler.add_function(func)
                    for f in follow:
                        profiler.add_function(f)
                    profiler.enable_by_count()
                    return func(*args, **kwargs)
                finally:
                    profiler.print_stats()
            return profiled_func
        return inner

except ImportError:
    def do_profile(follow=[]):
        "Helpful if you accidentally leave in production!"
        def inner(func):
            def nothing(*args, **kwargs):
                return func(*args, **kwargs)
            return nothing
        return inner


def deprecated(func):
    '''This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used.
    '''
    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.warn_explicit(
            "Call to deprecated function {}.".format(func.__name__),
            category=DeprecationWarning,
            filename=func.func_code.co_filename,
            lineno=func.func_code.co_firstlineno + 1
        )
        return func(*args, **kwargs)
    return new_func

