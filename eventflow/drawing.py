import datetime
import math

import matplotlib.pyplot as plt
import matplotlib.colors as clr
import matplotlib.patches as mpatches
from matplotlib.offsetbox import TextArea, AnnotationBbox
import pandas as pd
from geopandas import GeoDataFrame
from shapely.geometry import Point
from shapely.ops import nearest_points

from eventflow.util import do_profile

class GraphLayer:
    """Manages all active graphs and the created matplotlib artists.
    The class provides some basic functionalities to enhance the 
    user experience, like hovering and on click.
    """
    def __init__(self, axes):
        """
        :param axes: Matplotlib axes
        :type axes: matplotlib.axes.Axes
        """
        self._edges = dict()
        self._actors = dict()
        self._axes = axes
        self._last_node = None
        self._cmap = plt.get_cmap('viridis')
        self.crs = {'init': 'epsg:4326'}
        node_structure = pd.DataFrame(columns = ["label", "WDid", "radius", "degree"])
        self._spatial_index = GeoDataFrame(node_structure, crs = self.crs, geometry = [])
        self.multi_point = self._spatial_index.geometry.unary_union

    def update(self, graph, actorID = None):
        """ Update the graph layer, by adding an additional graph.

        :param graph: Event graph
        :type graph: eventflow.EventGraph
        :param actorID: associated actor
        :type actorID: int
        """
        if graph.empty:
            return

        if actorID is not None:
            self._actors[actorID] = []

        
        for key, edge in graph.edges.iterrows():
            try:
                ec = clr.hex2color(edge.color)
            except:
                ec = "b"
            from_node = graph.nodes.ix[edge.from_node]
            to_node = graph.nodes.ix[edge.to_node]
            edge_patch = Edge(from_node = from_node, to_node = to_node, facecolor=ec, edgecolor=ec, zorder = 1)
            eid = self._axes.add_artist(edge_patch)
            self._edges[edge._id] = eid
            if actorID:
                self._actors[actorID].append(edge._id)

            self._add_node(from_node, actorID)
            self._add_node(to_node, actorID)
        
        # A little bit hacky, because I rely on the fact the the edges are ordered and the reference to the last edge still exists
        self._spatial_index.loc[edge.to_node, "radius"] += 1
        self.multi_point = self._spatial_index.geometry.unary_union

    def hide_by_actor(self, actorID):
        """ Remove the actor and the associated graph from the axes.

        :param actorID: ID of the actor
        :type actorID: int
        """
        # TODO: Node color is not updated properly
        edge_ids = self._actors.pop(actorID, [])
        for edge_id in edge_ids:
            edge = self._edges.pop(edge_id)
            self._spatial_index.loc[edge.to_node, "degree"] -= 1
            self._spatial_index.loc[edge.from_node, "degree"] -= 1

            edge.remove()
        self._spatial_index = self._spatial_index[self._spatial_index.degree > 0]
        self.plot()

    def plot(self):
        """Plot the axes."""
        try:
            self._nids.remove()
        except:
            pass
        
        if not self._spatial_index.empty:
            x = [p.x for p in self._spatial_index.geometry]
            y = [p.y for p in self._spatial_index.geometry]            
            radius = [self._actual_radius(n) for n in self._spatial_index.radius]

            try:
                color = [c for c in self._spatial_index.color]
            except:
                color = "b"
            self._nids = self._axes.scatter(x, y, marker = "o", s = radius, c = color, cmap = self._cmap) 
        self._axes.figure.canvas.draw()

    def show_annotation(self):
        """If a node was internally set, an annotation box will show up
        for that node."""
        try:
            ab = self._setup_annotation(self._last_node)
            self.annotation_artist = self._axes.add_artist(ab)
        except:
            print("Failed to show annotation box")
        self._axes.figure.canvas.draw_idle()

    def hide_annotation(self):
        """If possible hide the annotation box."""
        try:
            self.annotation_artist.remove()
        except:
            pass
        self._axes.figure.canvas.draw_idle()

    def hover(self, event, tol = 5): 
        """Callback function for mplcallback.onmove.
        This function will set the last_node if it was in range.

        :param event: mouse move event
        :type event: matplotlib.backend_bases.MouseEvent
        :param tol: tolerance in pixel
        :type tol: int
        """
        if event.xdata is None or event.ydata is None:
            return 
        if not self._spatial_index.empty:
            idx = self._nearest_point((event.xdata, event.ydata))
            if idx is not None and idx != self._last_node:              
                self.hide_annotation()
                self._last_node = idx
                self.show_annotation()
            else:
                self.hide_annotation()
                self._last_node = None

    def on_press(self, event):
        """Callback function for on_click event.

        :param event: matplotlib on press event
        :type event: matplotlib.backend_bases.MouseEvent
        """
        if event.button == 1 and event.xdata is not None and event.ydata is not None:
            idx = self._nearest_point((event.xdata, event.ydata))
            if idx is not None:
                return self._spatial_index.ix[idx]
        return None


    def _add_node(self, node, actorID):
        if node.name in self._spatial_index.index:
            #TODO: Currently the color is not overridden
            # I have to check what color represents the last visit
            self._spatial_index.loc[node.name, "degree"] += 1
            if self._spatial_index.loc[node.name, "color"] < node.color:
                self._spatial_index.loc[node.name, "color"] = node.color
                
        else:
            row = GeoDataFrame([[node.label, node.WDid, 1, 1, node.color]], index = [node.name], columns = ["label", "WDid", "radius", "degree", "color"],
                geometry = [Point(node.lat, node.lon)], crs = self.crs)
            self._spatial_index = self._spatial_index.append(row)

    def _setup_annotation(self, nodeID):
        node = self._spatial_index.ix[nodeID]
        offsetbox = TextArea(node.label, minimumdescent=False)

        ab = AnnotationBbox(offsetbox, (node.geometry.x, node.geometry.y),
            xybox=(-20, 20),
            xycoords='data',
            boxcoords="offset points",
            arrowprops=dict(arrowstyle="->"))
        return ab

    def _nearest_point(self, xy, tol = 5):
        point = Point(xy[0], xy[1])
        nearest = nearest_points(point, self.multi_point)
        if nearest[0].distance(nearest[1]) < tol:
            try:
                nn = self._spatial_index[self._spatial_index['geometry']==nearest[1]]
                idx = nn.index.tolist()[0]
                return idx
            except:
                print("Failed to compute nearest point: {}".format([o.wkt for o in nearest]))
        else:
            return None

    def _actual_radius(self, x, s = 15, k = 0.1):
        """Compute the radius of the markers as an exponential growth 
        limited by an upper bound s. Growth factor k determines the convergence speed.
        """
        result = s - ((s - 2)*math.exp(-k*x))
        return result

class Edge(mpatches.FancyArrowPatch):
    """Wrapper class for the matplotlib.patches.FancyArrowPatch.
    The edge will be styled to display a nice directed edge.
    The edge will keep track of the starting and end node.
    """

    def __init__(self, **kwargs):
        """
        :param from_node: Starting node
        :type from_node: pandas.Series
        :param to_node: End node
        :type to_node: pandas.Series
        
        All other kwargs are passed to matplotlib.patches.FancyArrowPatch
        """

        from_node = kwargs.pop("from_node")
        to_node = kwargs.pop("to_node")

        self.from_node = from_node.name
        self.to_node = to_node.name

        arrow_style = mpatches.ArrowStyle("-|>", head_length=3, head_width=3)
        kwargs["arrowstyle"] = arrow_style
        kwargs["posA"] = tuple(from_node[["lat","lon"]])
        kwargs["posB"] = tuple(to_node[["lat","lon"]])
        
        super(Edge,self).__init__(**kwargs) 

        



