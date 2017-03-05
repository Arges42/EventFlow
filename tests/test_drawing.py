import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.testing.decorators import image_comparison
import pytest

import eventflow
from eventflow.util import adrastea
from eventflow.drawing import GraphLayer
try:
    from mpl_toolkits.basemap import Basemap
    skip_basemap = False
except:
    skip_basemap = True

@pytest.mark.mpl_image_compare
def test_graph_layer():
    @adrastea()
    def drawing(env):
        client = env["client"]
            
        fig = plt.figure('graph_layer')
        axes = fig.add_subplot(111)
        axes.set_title(fig.get_label())

        actorID = 2936
        gl = GraphLayer(axes)
        gc = eventflow.GraphCollection(actorID, client)
        for actor, graph in gc.graphs():
            nodes, edges = graph.build()
            nodes.population = 0
            gl.update(nodes, edges, actorID = actor.id)
        gl.update(nodes, edges)

        gl.plot()
        return fig

    return drawing()

@pytest.mark.skipif(skip_basemap, reason = "Could not import mpl_toolkits.basemap")
@pytest.mark.mpl_image_compare
def test_graph_layer_w_basemap():

    @adrastea()
    def drawing(env):
        client = env["client"]
            
        fig = plt.figure('graph_layer_w_basemap')
        axes = fig.add_subplot(111)
        axes.set_title(fig.get_label())

        m = Basemap(ax=axes)
        m.drawcountries(color='#6cb0e0')
        m.fillcontinents(color='#000000',zorder=0,lake_color='#000000')
        m.drawmapboundary(fill_color='#4c4c4c')

        actorID = 2936
        gl = GraphLayer(axes)
        gc = eventflow.GraphCollection(actorID, client)
        for actor, graph in gc.graphs():
            nodes, edges = graph.build()
            nodes.population = 0
            gl.update(nodes, edges, actorID = actor.id)
        gl.update(nodes, edges)

        gl.plot()
        return fig
    return drawing()


