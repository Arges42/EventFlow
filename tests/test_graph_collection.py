import sys
import random
import pandas as pd
import numpy as np
import datetime
from pandas.util.testing import assert_frame_equal

sys.path.append("../src")

import eventflow
from eventflow.util import adrastea
from eventflow import util
from eventflow import db_queries


@adrastea()
def test_graph_collection_actors(env):
    client = env["client"]

    actors_with_id = [0, 2]
    actors_with_wdid = ["Q168261", "Q160362"]
    actors_with_labels = ["Ptolemy I Soter", "Theophrastus"]

    gc = eventflow.GraphCollection(actors_with_id, client)
    gc2 = eventflow.GraphCollection(actors_with_wdid, client)
    gc3 = eventflow.GraphCollection(actors_with_labels, client)

    actors = [a.id for a in gc.actors]
    actors.sort()

    actors2 = [a.id for a in gc2.actors]
    actors2.sort()

    actors3 = [a.id for a in gc3.actors]
    actors3.sort()
    assert actors == actors_with_id
    assert actors2 == actors_with_id
    assert actors3 == actors_with_id

@adrastea()
def test_graph_collection_graps(env):
    client = env["client"]
    
    actorID = 2936

    gc = eventflow.GraphCollection(actorID, client)

    nodes, edges = db_queries.get_graph(client, actorID)
    nodes = nodes.set_index("locationID")
    nodes.sort_index(inplace=True)
    edges = edges.sort_values("from_date")
    edges.from_date = edges.from_date.apply(lambda x: datetime.date(*[int(d) for d in x.split("-")]))
    edges.to_date = edges.to_date.apply(lambda x: datetime.date(*[int(d) for d in x.split("-")]))
    
    actor, graph = next(gc.graphs())
    
    
    assert_frame_equal(graph.nodes, nodes)
    assert_frame_equal(graph.edges, edges)

@adrastea()
def test_graph_collection_cache(env):
    client = env["client"]    
    actorID = 2936

    gc = eventflow.GraphCollection(actorID, client)

    actor, graph = next(gc.graphs())
    graph_c = gc.get_cache_entry(actorID)

    assert graph == graph_c
