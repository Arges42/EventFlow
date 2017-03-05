from collections import Iterator
import datetime
import os

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as clr

from . import util
from . import db_queries

NODES_SCHEMA = {"columns": ["label", "WDid", "lat", "lon"],
                "index": pd.Int64Index([], name="locationID"),
                "dtype": {"label": "object", "WDid": "object",
                          "lat": "float", "lon": "float"}
                }
EDGES_SCHEMA = {"columns": ["actorID", "from_node", "from_date",
                            "to_node", "to_date"],
                "index": pd.Int64Index([]),
                "dtype": {"actorID": "object", "from_node": "object",
                          "from_date": "object", "to_node": "object",
                          "to_date": "object"}
                }

EDGES_ESSENTIAL_COLUMNS = ["from_node", "from_date", "to_node", "to_date"]


class EventGraphError(Exception):
    """Basic EventGraph exception."""
    pass


class InputEventGraphWarning(EventGraphError):
    """Raise if input seems wrong."""
    pass


class Actor:
    """Convenience class to enable an easy Actor manipulation and querying."""
    def __init__(self, actor_id, client=None):
        """If no client is given, nothing will happen.

        :param actor_id: Id of the actor
        :type actor_id: int
        :param client: MongoDB client
        :type client: pymongo.MongoClient
        """
        if client:
            self._parse_input(actor_id, client)

    def to_csv(self, directory=os.getcwd()):
        """Export the actorID, name and wikidataID to a csv file.
        The file will be named actorID_actor.csv.

        :param directory: Output directory
        :type directory: string
        """
        filename = os.path.join(directory, "{}_actor.csv".format(self.id))
        with open(filename, "w") as f:
            f.write("actorID,{}\n".format(self.id))
            f.write("name,{}".format(self.name))
            f.write("WDid,{}".format(self.WDid))

    @staticmethod
    def from_csv(filename, sep=";"):
        """Read a csv file and import the attributes.

        :param filename: Input filename
        :type filename: string
        :param sep: Separator used in the csv
        :type sep: string
        """
        with open(filename, "r") as f:
            for line in f.readlines():
                key, val = line.strip().split(sep)
                setattr(self, key, val)

    def _parse_input(self, actor_string, client):

        if(self._is_int(actor_string)):
            query_function = db_queries.actor_by_id
            aid = int(actor_string)
        elif(self._is_WDid(actor_string)):
            query_function = db_queries.actor_by_WDid
            aid = actor_string
        else:
            query_function = db_queries.actor_by_name
            aid = actor_string

        self._query_actor(aid, query_function, client)

    def _set_properties(self, actor_dict):
        for key, val in actor_dict.items():
            setattr(self, key, val)

    def _query_actor(self, actor_id, query_function, client):
        actor = query_function(client, actor_id)
        self._set_properties(actor)

    def _is_int(self, val):
        try:
            int(val)
            return True
        except:
            return False

    def _is_WDid(self, val):
        try:
            if(val[0] == "Q"):
                int(val[1:])
                return True
            else:
                return False
        except:
            return False

    def __repr__(self):
        try:
            props = "id = {}, WDid = {}, name = {}".format(
                self.id, self.WDid, self.name)
            return "{}.{}({})".format(
                self.__module__, self.__class__.__name__, props)
        except:
            return self.__class__


class GraphCollection:
    """
    Manages the actors and their resepective EventGraph's.
    Queries the data from the database and stores them in a cache.
    """

    def __init__(self, actor_list, client, load_function=db_queries.get_graph):
        """
        :param actor_list: List of actors, for which to fetch the EventGraph
        :type actor_list: list of either actor ids, wikidata ids
            or string labels
        :param client: The client for the database access
        :type client: pymongo.MongoClient
        :param load_function: Function which queries for the nodes and edges
        :type load_function: function -> load_function(client, actorID):
            return nodes, edges
        """
        self._actors = dict()
        self._cache = dict()
        self.client = client
        self.load_function = load_function

        self.update_actor_list(actor_list)

    def graphs(self):
        """
        Generator for the graphs.
        If an EventGraph is not stored in the cache,
        it is fetched with _query_graph.
        """
        for index, actor in self._actors.items():
            if actor.id in self._cache:
                yield (actor, self._cache[actor.id])
            else:
                graph = self._query_graph(actor_id=actor.id)
                if graph:
                    self._cache[actor.id] = graph
                    yield (actor, graph)

    def clear(self):
        """ Clear everything. Empties the cache and the saved actors."""
        for graph in self._cache.values():
            del graph
        for actor in self._actors.values():
            del actor
        self._actors = dict()
        self._cache = dict()

    def update_actor_list(self, actor_list):
        """
        Update the internal list of actors.

        :param actor_list: Single actorID or list of actorIDs
        :type actor_list: int or list
        """
        if not isinstance(actor_list, list):
            actor_list = [actor_list]

        for a in actor_list:
            if not isinstance(a, Actor):
                actor = str(a).strip()
                actor = Actor(actor, self.client)
                if (actor.id not in self._actors and actor.id != -1):
                    self._actors[actor.id] = actor

    def remove_actor(self, actor_id):
        """Remove an actor from the collection, if a cached graph
        is associated with the actorID it is also deleated.

        :param actor_id: Id of the actor
        :type actor_id: int
        """
        try:
            actor = self._actors.pop(actor_id)
            del actor
        except:
            pass
        try:
            graph = self._cache.pop(actor_id)
            del graph
        except:
            pass

    def get_actor(self, actor_id):
        """Get an actor by its id.

        :param actor_id: Id of the actor
        :type actor_id: int
        """
        return self._actors.get(actor_id, None)

    def get_cache_entry(self, actor_id):
        """Get a cached EventGraph with the actorID.

        :param actor_id: Id of the actor
        :type actor_id: int
        """
        return self._cache.get(actor_id, None)

    def add(self, actor, graph):
        """Add a graph with its associated actor to the graph collection.

        :param actor: Associated actor
        :type actor: eventflow.core.Actor
        :param graph: EventGraph
        :type graph: eventflow.core.EventGraph
        """
        if not isinstance(actor, Actor):
            raise EventGraphError("""Input actor
                must be an instance of {}.{}.""".format(Actor.__module__,
                                                        Actor.__name__))
        if not isinstance(graph, EventGraph):
            raise EventGraphError("""Input graph
                must be an instance of {}.{}.""".format(EventGraph.__module__,
                                                        EventGraph.__name__))

        self._actors[actor.id] = actor
        self._cache[actor.id] = graph

    @property
    def actors(self):
        """List of actors."""
        return list(self._actors.values())

    @property
    def num_actors(self):
        return len(self._actors)

    def _query_graph(self, actor_id):
        if actor_id != -1:
            nodes, edges = self.load_function(self.client, actor_id)
            if nodes.empty or edges.empty:
                return None
            graph = EventGraph(nodes, edges)

            return graph
        else:
            return None


class EventGraph:
    """Base class for a single event graph.
    It manages the nodes andd edges for one specific actor.
    """
    def __init__(self, nodes, edges):
        """
        :param nodes: Data frame which represents the set of nodes,
            should at least contain the columns [index_column, lat, lon]
        :type nodes: pandas.DataFrame
        :param edges: Data frame which represents the set of edges,
            should at least contain the columns
            [from_node, from_date, to_node, to_date]
        :type edges: pandas.DataFrame
        :param index_column: Name of the index column of the nodes
        :type index_column: str
        """
        if not (isinstance(nodes, pd.DataFrame) and
                isinstance(edges, pd.DataFrame)):

            raise EventGraphError("""Input types for EventGraph
                need to be pandas.DataFrame""")

        if not set(EDGES_ESSENTIAL_COLUMNS) < set(edges.columns):
            raise EventGraphError("""Missing essential columns for the edges.
                Need at least: {}""".format(EDGES_ESSENTIAL_COLUMNS))

        self._nodes = nodes
        self._edges = edges

        if self._nodes.empty or self._edges.empty:
            self._nodes, self._edges = empty_graph_data()

        if self._nodes.index.name != NODES_SCHEMA["index"].name:
            self._nodes = self._nodes.set_index(NODES_SCHEMA["index"].name)

        try:
            self._edges.from_date = self._edges.from_date.apply(
                lambda x: datetime.date(*[int(d) for d in x.split("-")]))
            self._edges.to_date = self._edges.to_date.apply(
                lambda x: datetime.date(*[int(d) for d in x.split("-")]))
        except AttributeError:
            pass

        self._edges.sort_values("from_date", inplace=True)

        self._cmap = plt.get_cmap('viridis')

        self.build(color_nodes=False, color_edges=False)

    def build(self,
              start_date=None,
              end_date=None,
              color_edges=True,
              color_nodes=True):
        """
        Set the valid time interval of the graph.
        Limit the number of edges and nodes

        :param start_date: yyyy-mm-dd (ISO 8601)
        :type start_date: String
        :param end_date: yyyy-mm-dd (ISO 8601)
        :type start_date: String
        :param color_edges: Whether or not to compute the edge color
            based on occurence days
        :type color_edges: bool
        :param color_nodes: Whether or not to compute the node color
            based on occurence days
        :type color_nodes: bool
        """

        if not start_date:
            self._start_date = self._edges.from_date.min()
        elif isinstance(start_date, datetime.date):
            self._start_date = start_date
        else:
            self._start_date = datetime.date(
                *[int(x) for x in start_date.split("-")])

        if not end_date:
            self._end_date = self._edges.to_date.max()
        elif isinstance(end_date, datetime.date):
            self._end_date = end_date
        else:
            self._end_date = datetime.date(
                *[int(x) for x in end_date.split("-")])

        self._reduce_graph()
        if self._reduced_edges.empty:
            return empty_graph_data()

        if color_edges:
            self._edge_color()
        if color_nodes:
            self._node_color()

        return self

    def coocurrence(self, graph):
        result = self._reduced_edges.merge(graph.edges, left_on=["from_node", "from_date"], right_on=["from_node", "from_date"], how="inner", suffixes=["","_x"])
        #cols = [x for x in result.columns if x.endswith("_x")]

        if not result.empty:
            result = result[["from_node", "from_date"]]
            result.columns = ["locationID", "date"]
            result = result.merge(self._reduced_nodes, left_on="locationID", right_index=True, how="left")
            return result[["locationID", "date", "label", "WDid"]]
        else:
            return None

    def intersect(self, graph):
        result = self._reduced_edges.merge(graph.edges, left_on=["from_node", "from_date", "to_node", "to_date"], right_on=["from_node", "from_date", "to_node", "to_date"], how="inner", suffixes=["","_x"])
        edges = result[["_id","from_node","to_node","from_date","to_date"]]
        nodes = pd.concat([self._reduced_nodes.ix[edges.from_node],self._reduced_nodes.ix[edges.to_node]]).drop_duplicates()
        nodes = nodes.drop(["color", "population"], 1)
        return EventGraph(nodes.reset_index(), edges)

    def to_csv(self, directory=os.getcwd(), prefix="", suffix=""):
        """Write nodes and edges to a direcotry. The files will include
        nodes/edges as part of their name.

        :param directory: Output directory
        :type directory: string
        :param prefix: Prefix for the saved files
        :type prefix: string
        :param suffix: Suffix for the saved files
        :type suffix: string
        """
        nodes_file = os.path.join(directory, prefix+"nodes"+suffix+".csv")
        self._reduced_nodes.to_csv(nodes_file)

        edges_file = os.path.join(directory, prefix+"edges"+suffix+".csv")
        self._reduced_edges.to_csv(edges_file, index=False)

    @property
    def nodes(self):
        """Active set of nodes."""
        return self._reduced_nodes

    @property
    def edges(self):
        """Active set of edges."""
        return self._reduced_edges

    @property
    def min_date(self):
        """Total minimum date."""
        return self._edges.from_date.min()

    @property
    def max_date(self):
        """Total maximum date."""
        return self._edges.to_date.max()

    @property
    def empty(self):
        """Does the graph contain node and edges."""
        return self._reduced_nodes.empty and self._reduced_edges.empty

    def _reduce_graph(self):
        """
        Limit the active set of nodes and edges to the current time frame,
        given by self._start_date and self._end_date

        Internally sets self._reduced_edges and self._reduced_nodes
        """
        if self._start_date and self._end_date:
            self._reduced_edges = self._edges[
                (self._edges.from_date >= self._start_date) &
                (self._edges.to_date <= self._end_date)]
        else:
            self._reduced_edges = self._edges

        self._reduced_edges = self._reduced_edges.sort_values("from_date")

        legit_nodes = self._reduced_edges[["from_node", "to_node"]].values
        legit_nodes = np.unique(legit_nodes).astype(int)
        self._reduced_nodes = self._nodes.ix[legit_nodes]

    def _edge_color(self):
        self._reduced_edges["color"] = self._reduced_edges.to_date.apply(
            lambda x: self._get_hex_color(x))

    def _node_color(self):
        last_visit = self._reduced_edges.groupby("to_node")["to_date"].max()
        color = last_visit.apply(lambda x: self._get_scaled_time_point(x))
        color.name = "color"

        self._reduced_nodes = self._reduced_nodes.merge(
            color.to_frame(), left_index=True, right_index=True,
            how="left")

        no_color = 0
        self._reduced_nodes.color.fillna(no_color, inplace=True)

    # TODO: Population is not that good
    def _node_population(self):
        last_stop = self._reduced_edges["to_date"].max()
        idx = self._reduced_edges.to_date == last_stop
        last_node = self._reduced_edges[idx].to_node.iloc[0]
        self._reduced_nodes["population"] = 0
        self._reduced_nodes.loc[last_node, "population"] = 1

    def _get_scaled_time_point(self, date):
        scale = date
        scale -= self._start_date
        try:
            scale = 1.*scale.days/(self._end_date-self._start_date).days
        except:
            scale = 0
        return scale

    def _get_hex_color(self, date):
        """ Compute the color on the set color map for a date in
        the set timeframe.
        Granularity is days.

        :param date: Zeitstempel
        :type start_date: datetime.date
        """
        scale = date
        scale -= self._start_date
        try:
            scale = 1.*scale.days/(self._end_date-self._start_date).days
        except:
            scale = 0
        rgb = self._cmap(scale)

        return clr.rgb2hex(rgb)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        try:
            pd.util.testing.assert_frame_equal(self.nodes, other.nodes)
            pd.util.testing.assert_frame_equal(self.edges, other.edges)
            return True
        except:
            return False

    def __ne__(self, other):
        if isinstance(other, self.__class__):
            return not self.__eq__(other)
        return NotImplemented


def empty_graph_data():
    """Build an empty event graph, based on the NODES and EDGES SCHEMA."""

    empty_nodes = pd.DataFrame(columns=NODES_SCHEMA["columns"],
                               index=NODES_SCHEMA["index"])
    for column, dtype in NODES_SCHEMA["dtype"].items():
        empty_nodes[column] = empty_nodes[column].astype(dtype)

    empty_edges = pd.DataFrame(columns=EDGES_SCHEMA["columns"],
                               index=EDGES_SCHEMA["index"])
    for column, dtype in EDGES_SCHEMA["dtype"].items():
        empty_edges[column] = empty_edges[column].astype(dtype)

    return empty_nodes, empty_edges


def from_csv(filename_nodes,
             filename_edges,
             node_kwargs=dict(),
             edge_kwargs=dict()):
    """Create an EventGraph from csv files.
    If the loaded columns are not identical to the columns specified in
    NODES_SCHEMA and EDGES_SCHEMA, the function will exit.
    The read functions tries to get the correct index columns for the nodes.

    :param filename_nodes: Filename containing the node data
    :type filename_nodes: string
    :param filename_edges: Filename containing the edge data
    :type filename_edges: string
    :param node_kwags: Kwargs for pd.read_csv (node file)
    :type node_kwags: dict
    :param edge_kwagss: Kwargs for pd.read_csv (edge file)
    :type edge_kwags: dict
    """
    try:
        nodes = pd.read_csv(filename_nodes, **node_kwargs)
    except:
        raise
        # raise EventGraphError("Could not read {}.".format(filename_nodes))
    if set(nodes.columns) == set(NODES_SCHEMA["columns"]):
        if nodes.index.name != NODES_SCHEMA["index"].name:
            nodes.index.name = NODES_SCHEMA["index"].name
            raise InputEventGraphWarning("""Index columns not specified,
                or not equal to the NODES_SCHEMA,
                renaming index column.""")
        graph_nodes = nodes
    elif (set(nodes.columns) ==
          set(NODES_SCHEMA["columns"]+[NODES_SCHEMA["index"].name])):

        graph_nodes = nodes.set_index(NODES_SCHEMA["index"].name)
    else:
        raise EventGraphError("""Columns of {} are not equal
            to the given NODES_SCHEMA""".format(filename_nodes))
        return None

    try:
        edges = pd.read_csv(filename_edges, **edge_kwargs)
    except:
        raise
        # raise EventGraphError("Could not read {}.".format(filename_edges))
    if set(edges.columns) == set(EDGES_SCHEMA["columns"]):
        graph_edges = edges
        for key, name in EDGES_SCHEMA["dtype"].items():
            graph_edges[key] = graph_edges[key].astype(name)
    else:
        raise EventGraphError("""Columns of {} are not equal
            to the given EDGES_SCHEMA""".format(filename_edges))
        return None

    return EventGraph(graph_nodes, graph_edges)
