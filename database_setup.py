"""Script to setup the eventflow graph database.
Author: Jan Greulich
"""

import sys

import numpy as np
import pandas as pd
import pymongo

from eventflow import util

def build_connections(triples):
    ''' This function builds the connections for one actor
        1. Group by year,month and day and select the maximum locationID (that should be the most refined event)
        2.Create a node for each distinct locationID
        3.Sort the events by date
        4.Create an edge betweent two adjacent events
    '''
    unique_dates = triples
    unique_dates = unique_dates.sort_values(["year", "month", "day"])
    unique_dates = unique_dates.reset_index()
    edges = pd.DataFrame(columns=["from_node", "to_node", "from_date", "to_date"])
    for index, row in unique_dates.iterrows():
        if(index < unique_dates.shape[0]-1):
            next_event = unique_dates.ix[index+1]
            from_date = "{}-{}-{}".format(row.year, 
                                          str(row.month).zfill(2),
                                          str(row.day).zfill(2))
            to_date = "{}-{}-{}".format(next_event.year,
                                        str(next_event.month).zfill(2),
                                        str(next_event.day).zfill(2))
            data = [row.locationID, next_event.locationID, from_date, to_date]
            tmp = pd.DataFrame([data],
                columns=["from_node", "to_node", "from_date", "to_date"])
            edges = edges.append(tmp)

    edges.from_node = edges.from_node.astype(int)
    edges.to_node = edges.to_node.astype(int)

    return edges


def get_real_dates(client, triples, merge=True):
    """ Transform the date id of the triples into an actual date.

    :param triples: Event triples
    :type triples: pandas.DataFrame
    :param client: mongodb client.
    :type client:
    :param merge: Merge with input.
    :type merge: bool

    :returns: pandas.DataFrame -- columns=[
        dateID: Links to the EventTripel.nodes collection. Where nodeType=DAT
        and nodeID=dateID.
        year:
        month:
        day:

    """

    nodes = client[util.EVENT_TRIPLES]["nodes"]
    # WARNING: list(pd.Series.unique()) will not work,
    # beause the internal list elements have the wrong type
    dateIDs = np.unique(triples.dateID.values).tolist()
    pipeline = [
        {"$match": {"nodeID": {"$in": dateIDs}, "$and": [{"nodeType": "DAT"}]}}
    ]
    dates = pd.DataFrame(list(nodes.aggregate(pipeline)))
    # Refine the dates
    years, months, days = util.date_parser(dates.nodeLabel)
    dates["year"] = years
    dates["month"] = months
    dates["day"] = days
    dates = dates.drop(["_id", "degree", "nodeLabel", "nodeType"], 1)
    dates.columns = ["dateID", "year", "month", "day"]

    if merge:
        dates = triples.merge(dates, left_on="dateID", right_on="dateID")
    return dates


def copy_location_nodes(client):
    """Copy the location nodes from EVENT_TRIPLES.nodes to my own collection.
    Use only nodes which have the location_type 'City'.

    The inserted triple will have the following schema:
    locationID: Int32 -> nodeId of EVENT_TRIPLES.nodes
    lat: double 
    lon: double
    WDid: Int32 -> WikidataID without the leading 'Q'
    label: String -> Wikidata norm_name
    """

    nodes = client["jgreulich"]["nodes"]
    event_nodes = client[util.EVENT_TRIPLES]["nodes"]
    wikidata = client[util.WIKIDATA]["WD_NEs"]

    locations = event_nodes.find({'nodeType':'LOC'},{'_id':0,'nodeID':1,'nodeLabel':1},no_cursor_timeout=True).sort('nodeID',pymongo.ASCENDING)
    coordinates_proj = {'_id':0,'coordinate':1,'norm_name':1,'id':1}
    nodeID = 0
    loc_count = locations.count()
    i = 0
    for item in locations:
        coordinates = wikidata.find({'id':int(item["nodeLabel"][1:]),'$and':[{'neClass':'LOC',"location_type":{"$in" : ["City","POI"]},"coordinate": {"$ne": []}}]},coordinates_proj)
        try:
            record = coordinates.next()
            record["locationID"] = item["nodeID"] #Maps to the locationID of the event triples
            record["lon"] = float(record["coordinate"][0].split(" ")[0])
            record["lat"] = float(record["coordinate"][0].split(" ")[1])
            record["label"] = record["norm_name"]
            record["WDid"] = record["id"] 
            del record["coordinate"]
            del record["norm_name"]
            del record["id"]
            nodes.insert_one(record)
        except StopIteration:
            pass
        i += 1
        sys.stdout.write("\rProgress: {0:.2f}%".format(100. * i / loc_count))
        sys.stdout.flush()
    del locations

def create_edge_collection(client):
    """
    Build an edge collection based on the event triples from EVENT_TRIPLES.triples
    and the nodes created with copy_location_nodes.
    All events which do not have a full date (e.g. missing one or more of day,
    month, year) are ignored. The events are grouped by actorID and sorted 
    ascending according to their date. Afterwards every event is connected to
    the following event.
    The edges have the following schema:
    actorID: Int32 -> Id according to EVENT_TRIPLES.nodes
    from_date: String -> Date of the starting event
    from_node: Int32 -> Id according to the locationID
    to_date: String -> Date of the follwing event
    to_node: Int32 -> Id according to the locationID
    """

    db_edges = client["jgreulich"]["edges"]
    db_nodes = client["jgreulich"]["nodes"]
    event_nodes = client[util.EVENT_TRIPLES]["nodes"]
    event_triples = client[util.EVENT_TRIPLES]["triples"]
    actors = event_nodes.find({"nodeType":"ACT"},no_cursor_timeout=True).sort("nodeID",pymongo.ASCENDING)
    act_count = actors.count()
    i = 0
    for actor in actors:
        triples = pd.DataFrame(list(event_triples.find({"actorID":actor["nodeID"]})))
        triples = get_real_dates(client, triples)
        triples = triples = triples[(triples.month!=0) & (triples.day!=0)]
        if(triples.size>0):
            legit_nodes = db_nodes.find({"locationID":{"$in":np.unique(triples.locationID.values).tolist()}},{"_id":0,"locationID":1})
            legit_nodes = pd.DataFrame(list(legit_nodes))
            triples = triples[triples.locationID.isin(legit_nodes.values.flatten())]
            if(triples.size>0):
                edges = build_connections(triples)
                if(edges.size>0):
                    edges["actorID"] = actor["nodeID"]
                    db_edges.insert_many(edges.to_dict(orient='records'))
        i+=1
        sys.stdout.write("\rProgress: {0:.2f}%".format(100. * i / act_count))
        sys.stdout.flush()
    del actors

@util.adrastea()
def main(env):
    client = env['client']
    copy_location_nodes(client)
    create_edge_collection(client)

if __name__ == "__main__":
    main()
