"""This module enables a high-level access to all database queries, 
which are needed in the core module. 
"""
import os
import sys

import pandas as pd
import numpy as np
import pymongo

from . import util
    
def get_graph(client, actorID):
    """Query the eventflow collection which is specified in conig.ini.
    :param client: mongodb client.
    :type client:
    :param actorID: actorID which can be used to identify the edges of one actor
    :type actorID: list, string or file

    :result: nodes, edges with all properties found in the database
    :type result: pd.DataFrame, pd.DataFrame
    """

    eventflow_nodes = client[util.EVENTFLOW_COLLECTION][util.EVENTFLOW_NODES]
    eventflow_edges = client[util.EVENTFLOW_COLLECTION][util.EVENTFLOW_EDGES]
    
    if isinstance(actorID,list):
        ids = actorID
    elif os.path.isfile(actorID):
        f = open(actorID, 'r')
        ids = [x.split(",")[0].strip() for x in f.readlines()]
    else:
        ids = [actorID]

    pipeline = [
        {"$match": {"actorID": {"$in": ids}}}
    ]

    result = eventflow_edges.aggregate(pipeline)
    result = list(result)
    edges = pd.DataFrame(result)
    if edges.empty:
        return pd.DataFrame(), edges

    locationIDs = np.unique(edges[["from_node","to_node"]].values.flatten()).tolist()
    pipeline = [
        {"$match": {"locationID": {"$in": locationIDs}}}
    ]
    nodes = pd.DataFrame(list(eventflow_nodes.aggregate(pipeline)))
    
    if nodes.empty:
        return nodes,pd.DataFrame()        
    
    edges = edges.merge(nodes[["locationID","lat","lon"]].add_prefix("from_"),left_on="from_node",right_on="from_locationID",how="left")
    edges = edges.merge(nodes[["locationID","lat","lon"]].add_prefix("to_"),left_on="to_node",right_on="to_locationID",how="left")
    edges = edges.drop(["to_locationID","from_locationID"],1)
    edges = edges.dropna()

    return nodes, edges
    

def actor_by_id(client, actor_id):
    event_nodes = client[util.EVENT_TRIPLES]["nodes"]
    triple_node = event_nodes.find_one({"nodeType":"ACT", "$and" :[{"nodeID":actor_id}]}) 

    result = {"id":actor_id,"WDid":triple_node["nodeLabel"][1:],"name":triple_node["WDlabel"]}
    return result

def actor_by_WDid(client,actor_WDid):
    event_nodes = client[util.EVENT_TRIPLES]["nodes"]

    triple_node = event_nodes.find_one({"nodeLabel":"{}".format(actor_WDid)})

    result = {"id":triple_node["nodeID"],"WDid":actor_WDid[1:],"name":triple_node["WDlabel"]}
    return result

def actor_by_name(client, actor_name):
    """ Get actor by name.
        This is ambigious, because there might be more 
        then one person with the same name.

    """
    event_nodes = client[util.EVENT_TRIPLES]["nodes"]

    triple_node = event_nodes.find_one({"WDlabel":actor_name})

    try:
        result = {"id":triple_node["nodeID"],"WDid":triple_node["nodeLabel"][1:],"name":actor_name}
    except:
        result = {"id":-1,"WDid":-1,"name":""}
    return result

