import sys
import os
import random
import pandas as pd
import numpy as np
import datetime

from pandas.util.testing import assert_frame_equal
import pytest

import eventflow

def data(num_nodes = 10, num_edges = 10, actorID = 1):
    nodes, edges = eventflow.EventGraph.empty_graph_data()
    
    for i in range(num_nodes):
        row = ["label{}".format(i), "Q{}".format(i), random.uniform(-90,90), random.uniform(-180,180)]
        nodes.loc[i] = row

    start_date = datetime.date(1900, 1, 1)
    for i in range(num_edges):
        timedelta = datetime.timedelta(days = random.randint(0,200))
        end_date = start_date + timedelta
        edge = [actorID, random.choice(range(num_nodes)), start_date, random.choice(range(num_nodes)), end_date]
        edges.loc[i] = edge
        start_date = end_date

    for col in edges.columns:
        edges[col] = edges[col].astype("object")
    nodes = nodes.ix[np.unique(edges[["from_node", "to_node"]].values).astype(int)]

    return nodes, edges


def test_event_graph_setup():
    nodes, edges = data() 
    #nodes2, edges2 = data(num_nodes = 0) 
    nodes3, edges3 = data(num_edges = 0)    
    nodes4, edges4 = data(num_edges = 0, num_nodes = 0)

    e = eventflow.EventGraph(nodes, edges)
    #e2 = eventflow.EventGraph(nodes2, edges2)
    e3 = eventflow.EventGraph(nodes3, edges3)
    e4 = eventflow.EventGraph(nodes4, edges4)
    
    nodes = nodes.sort_index()    
    #nodes2 = nodes2.sort_index()
    nodes3 = nodes3.sort_index()
    nodes4 = nodes4.sort_index()

    assert_frame_equal(e.nodes, nodes)
    assert_frame_equal(e.edges, edges)

    #assert_frame_equal(e2.nodes, nodes2)
    #assert_frame_equal(e2.edges, edges2)

    assert_frame_equal(e3.nodes, nodes3)
    assert_frame_equal(e3.edges, edges3)

    assert_frame_equal(e4.nodes, nodes4)
    assert_frame_equal(e4.edges, edges4)

    

def test_event_graph_properties():
    nodes, edges = data()

    e = eventflow.EventGraph(nodes, edges)

    assert e.min_date == edges.from_date.min()
    assert e.max_date == edges.to_date.max()
    assert isinstance(e.min_date, datetime.date)
    assert isinstance(e.max_date, datetime.date)


def test_event_graph_empty():
    nodes, edges = data()

    e = eventflow.EventGraph(pd.DataFrame(columns = nodes.columns), pd.DataFrame(columns = edges.columns))
        
    assert e.empty

def test_event_graph_comparison():
    nodes, edges = data()
    nodes2, edges2 = data(num_edges = 9)

    e = eventflow.EventGraph(nodes, edges)
    e2 = eventflow.EventGraph(nodes2, edges2)
    e3 = eventflow.EventGraph(pd.DataFrame(columns = nodes.columns), pd.DataFrame(columns = edges.columns))

    assert e == e
    assert e != e2
    assert e3 == e3

def test_event_graph_build_no_timeframe():
    """ 
    Test if the build method does not change anything 
    if no timeframe is supplied.
    """
    nodes, edges = data()
    e = eventflow.EventGraph(nodes, edges)

    before_nodes = e.nodes
    before_edges = e.edges

    after_nodes, after_edges = e.build(start_date = None, 
        end_date = None, 
        color_edges = False, 
        color_nodes = False)

    assert_frame_equal(before_nodes, after_nodes)
    assert_frame_equal(before_edges, after_edges)

def test_event_graph_build_w_timeframe(): 
    nodes, edges = data()   
   
    e = eventflow.EventGraph(nodes, edges)

    before_nodes = e.nodes
    before_edges = e.edges

    start_date = e.min_date
    end_date = e.max_date
    day_diff = e.max_date - e.min_date
    timedelta = datetime.timedelta(days = random.randint(0, day_diff.days))
    start_date = start_date + timedelta
    end_date = end_date - timedelta

    after_nodes, after_edges = e.build(start_date, end_date, False, False)
    
    before_edges = before_edges[(before_edges.from_date>=start_date) & (before_edges.to_date<=end_date)]
    before_nodes = before_nodes.ix[np.unique(before_edges[["from_node", "to_node"]].values).astype(int)]

    assert_frame_equal(before_nodes, after_nodes)
    assert_frame_equal(before_edges, after_edges)

def test_event_graph_read_write():

    nodes, edges = data()   
   
    e = eventflow.EventGraph(nodes, edges)

    direc = os.path.dirname(os.path.abspath(__file__))
    e.to_csv(directory = os.path.join(direc, "baseline"))

    direc = os.path.join(direc, "baseline")
    e2 = eventflow.from_csv(os.path.join(direc, "nodes.csv"), os.path.join(direc, "edges.csv"))

    print(e.edges)
    print(e2.edges)
    assert e == e2

def test_event_graph_errors():
    nodes, edges = data() 

    with pytest.raises(eventflow.EventGraphError):
        eventflow.EventGraph([], [])

    with pytest.raises(eventflow.EventGraphError):
        eventflow.EventGraph(nodes, [])

    with pytest.raises(eventflow.EventGraphError):
        eventflow.EventGraph([], edges)

    edges = edges.drop(["from_node"], 1)
    with pytest.raises(eventflow.EventGraphError):
        eventflow.EventGraph(nodes, edges)
    
