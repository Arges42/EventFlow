import sys
import os
import math
from itertools import chain
import itertools
import datetime
import tarfile

import matplotlib
matplotlib.use("Qt5Agg")
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
from matplotlib.figure import Figure
import matplotlib.pyplot as plt


from PyQt5.QtCore import Qt, pyqtSignal, QDate
from PyQt5 import QtCore
from PyQt5.QtWidgets import (QApplication, QMainWindow, QMenu, QVBoxLayout, 
    QWidget, QGridLayout, QHBoxLayout, QPushButton, QLineEdit, QListWidget, 
    QListWidgetItem, QDockWidget, QTextEdit, QStatusBar, QAction, 
    QAbstractItemView, QDateTimeEdit, QFileDialog)
from PyQt5.QtGui import QGuiApplication, QPainter, QLinearGradient, QColor, QBrush

import pandas as pd
from mpl_toolkits.basemap import Basemap

import eventflow 
from eventflow.util import adrastea
from eventflow.drawing import GraphLayer

class MyNavigationToolbar(NavigationToolbar):
    def __init__(self, canvas, parent, coordinates=True):
        super(MyNavigationToolbar, self).__init__(canvas, parent, coordinates)

    def zoom(self, *args):
        super(MyNavigationToolbar, self).zoom(*args)

class TimeBar(QWidget):
    def __init__(self, parent = None, start_time = None, end_time = None, colormap = "viridis"):
        super(TimeBar,self).__init__(parent)
        self.start_time = start_time
        self.end_time = end_time
        self._cmap = plt.get_cmap(colormap)
        

    def paintEvent(self, event):
        painter = QPainter()
        painter.begin(self)

        bar = self.rect()
        bar.setHeight(bar.height()*0.5)
        bar.setWidth(bar.width()*0.9)
        bar.translate(self.rect().width()*0.05,0)

        gradient = QLinearGradient(bar.topLeft(),bar.bottomRight())
        color = QColor()
        color.setRgbF(*self._cmap(0))
        gradient.setColorAt(0,color)
        color.setRgbF(*self._cmap(127))
        gradient.setColorAt(0.5,color)
        color.setRgbF(*self._cmap(255))
        gradient.setColorAt(1,color)
        painter.setBrush(gradient)
        painter.drawRect(bar)

        text_field = self.rect()
        text_field.setHeight(self.rect().height()*0.4)
        text_field.translate(0,self.rect().height()*0.6)
        painter.setPen(QColor())
        painter.drawText(text_field, Qt.AlignLeft, self.start_time)
        middle_time = (datetime.date(*[int(x) for x in self.end_time.split("-")]) 
                        - datetime.date(*[int(x) for x in self.start_time.split("-")]))
        middle_time = datetime.date(*[int(x) for x in self.start_time.split("-")])+middle_time/2
        painter.drawText(text_field, Qt.AlignCenter, middle_time.isoformat())
        painter.drawText(text_field, Qt.AlignRight, self.end_time)
        
        painter.end()


class WorldMap(QWidget):

    node_details = pyqtSignal(pd.Series)
    actor_inserted = pyqtSignal(eventflow.Actor)
    processing = pyqtSignal(float)

    def __init__( self, parent = None, client = None):        
        super( WorldMap, self ).__init__( parent)

        self.standard_xlim = (-180,180)
        self.standard_ylim = (-90,90)

        self.gc = eventflow.GraphCollection([3956, 3885, 53305, 48947], client)

        self.gridLayout = QGridLayout(self)

        self.gridLayout.setObjectName("gridLayout")
        self.widgetPlot = QWidget()
        self.widgetPlot.setObjectName("widgetPlot")        
        self.figure = Figure()        
        self.axes = self.figure.add_subplot(111)
        self.canvas = FigureCanvas( self.figure )
        
        self.canvas.setFocusPolicy( QtCore.Qt.ClickFocus )
        self.canvas.setFocus()
        #cid = self.canvas.mpl_connect('button_press_event', self.onclick)
        self.canvas.mpl_connect('motion_notify_event', self.hover)
        self.canvas.mpl_connect('button_press_event', self.on_press)

        layout = QVBoxLayout()
        self.widgetPlot.setLayout(layout)
        layout.addWidget(self.canvas)        
        self.gridLayout.addWidget(self.widgetPlot, 0, 0, 1, 1)
        self.mpl_toolbar = MyNavigationToolbar(self.canvas, self, coordinates = False)
        self.gridLayout.addWidget(self.mpl_toolbar, 1, 0, 1, 1)        

        self.m = Basemap(ax=self.axes)
        self.m.drawcountries(color='#6cb0e0')
        self.m.fillcontinents(color='#000000',zorder=0,lake_color='#000000')
        self.m.drawmapboundary(fill_color='#4c4c4c')

        self.graph_layer = GraphLayer(axes = self.axes)
        
        self.figure.subplots_adjust(left=0,right=1,bottom=0,top=1)
        self.axes.axis("tight")
        self.axes.axis('off')
        self.canvas.draw()



    def draw_network(self, start_date=None, end_date=None):
        self.axes.clear()

        self.m = Basemap(ax=self.axes)
        self.m.drawcountries(color='#6cb0e0')
        self.m.fillcontinents(color='#000000',zorder=0,lake_color='#000000')
        self.m.drawmapboundary(fill_color='#4c4c4c')
        self.canvas.draw()

        self.graph_layer = GraphLayer(axes = self.axes)
        num_actors = self.gc.num_actors
        processed = 0.
        actor_states = dict()
        for i in range(self.parent().parent().actor_overview.count()):
            a = self.parent().parent().actor_overview.item(i)
            actor_states[a.data(32)] = a.checkState()

        for actor, graph in self.gc.graphs():
            if actor_states.get(actor.id, 0) == 2: #TODO: Check if the actor is active
                graph = graph.build(start_date, end_date)
                self.graph_layer.update(graph, actor.id)
                self.graph_layer.plot()
                QGuiApplication.processEvents()
            processed += 1.
            self.processing.emit(processed/num_actors)

    def find_cooccurence(self, actor1, actor2):
        graph = self.gc.get_cache_entry(actor1)
        graph2 = self.gc.get_cache_entry(actor2)
        coocurrence = graph.coocurrence(graph2)
        intersection = graph.intersect(graph2)

        gl = GraphLayer(self.axes) 
        gl.update(intersection, -1)
        
        print(intersection.edges)

    def show_actor(self, actorID, start_date, end_date):
        graph = self.gc.get_cache_entry(actorID)
        if graph:
            graph = graph.build(start_date,end_date)
            self.graph_layer.update(graph, actorID)  
            self.graph_layer.plot()          

    def hide_actor(self,actorID):
        self.graph_layer.hide_by_actor(actorID)

    def hover(self, event):
        self.graph_layer.hover(event)

    def on_press(self, event):
        node = self.graph_layer.on_press(event)
        if node is not None:
            self.node_details.emit(node)



class MainWindow(QMainWindow):
    def __init__(self,client):
        super().__init__()
        self.client = client
        self.redrawing = False


        self.initUI()
        #self.worker = Worker(self)
        #self.map_explorer.draw_network()

    def initUI(self):
        mainWidget = QWidget(self)
        self.setCentralWidget(mainWidget)

        self.status_bar = QStatusBar(self)
        self.setStatusBar(self.status_bar)
        # Define child widgets
        grid = QGridLayout()
        mainWidget.setLayout(grid)

        self.map_explorer = WorldMap(self,self.client)
        grid.addWidget(self.map_explorer)

        layout_widget = QWidget(self)
        vLayout = QVBoxLayout()
        extra_navigation = QWidget(self)
        hLayout = QHBoxLayout()

        #self.slider = QRangeSlider()
        #hLayout.addWidget(self.slider)

        self.start_date = QDateTimeEdit()
        self.start_date.setDisplayFormat("yyyy-MM-dd")
        self.start_date.setMinimumDate(QDate(100,1,1))
        hLayout.addWidget(self.start_date)

        self.end_date = QDateTimeEdit()
        self.end_date.setDisplayFormat("yyyy-MM-dd")
        self.end_date.setMinimumDate(QDate(100,1,1))
        hLayout.addWidget(self.end_date)

        self.actor_select = QLineEdit()
        hLayout.addWidget(self.actor_select)

        self.fetch_data = QPushButton("Fetch Data")
        self.fetch_data.clicked.connect(self.reload_data)
        hLayout.addWidget(self.fetch_data)

        self.refresh = QPushButton("Refresh")
        self.refresh.clicked.connect(self.redraw)
        hLayout.addWidget(self.refresh)

        extra_navigation.setLayout(hLayout)
                
        vLayout.addWidget(extra_navigation) 

        self.time_bar = TimeBar(self)
        vLayout.addWidget(self.time_bar) 

        layout_widget.setLayout(vLayout)

        grid.addWidget(layout_widget)

        # Menubar
        self.saveAction = QAction('&Save state',self)
        self.saveAction.triggered.connect(self.save_state)
        self.loadAction = QAction('&Load state',self)
        self.loadAction.triggered.connect(self.load_state)

        menubar = self.menuBar()
        menubar.setNativeMenuBar(False)
        fileMenu = menubar.addMenu('&File')
        fileMenu.addAction(self.saveAction)
        fileMenu.addAction(self.loadAction)


        # Dock widgets
        dock = QDockWidget("Actors", self)
        dock.setAllowedAreas(Qt.LeftDockWidgetArea)

        container = QWidget(self)
        vLayout = QVBoxLayout()

        self.actor_overview = QListWidget(dock)
        self.actor_overview.setContextMenuPolicy(Qt.CustomContextMenu)  
        self.actor_overview.setSelectionMode(QAbstractItemView.ExtendedSelection)
        vLayout.addWidget(self.actor_overview)

        self.compare_actors = QPushButton("Compare selected")
        self.compare_actors.clicked.connect(self.compare_selected_actors)
        vLayout.addWidget(self.compare_actors)

        container.setLayout(vLayout)
        dock.setWidget(container)
        dock.setMinimumSize(150,0)
        self.addDockWidget(Qt.LeftDockWidgetArea, dock)

        dock = QDockWidget("Details", self)
        dock.setAllowedAreas(Qt.RightDockWidgetArea)

        container = QWidget(self)
        vLayout = QVBoxLayout()

        self.node_detail = QTextEdit(self)
        vLayout.addWidget(self.node_detail)

        container.setLayout(vLayout)
        dock.setWidget(container)
        dock.setMinimumSize(150,0)
        self.addDockWidget(Qt.RightDockWidgetArea, dock)

        # Slots
        self.actor_overview.itemChanged.connect(lambda x:self.change_visible_actors(x))
        self.actor_overview.customContextMenuRequested.connect(lambda x: self.actor_context_menu(x))
        self.map_explorer.node_details.connect(lambda x: self.display_node_details(x))
        self.map_explorer.processing.connect(lambda x:self.update_status_bar(x))

        self.show()

    def redraw(self):
        self.status_bar.showMessage("Redrawing...")
        self.redrawing = True
        start_date = self.start_date.dateTime().toString("yyyy-MM-dd")
        end_date = self.end_date.dateTime().toString("yyyy-MM-dd")
        #self.actor_overview.clear()

        self.time_bar.start_time = start_date
        self.time_bar.end_time = end_date
        self.time_bar.repaint()

        active_actors = []
        for i in range(self.actor_overview.count()):
            active_actors.append(self.actor_overview.item(i).data(32))
        
        for actor in self.map_explorer.gc.actors:
            if actor.id not in active_actors:
                item = QListWidgetItem(actor.name)
                item.setCheckState(Qt.Checked)
                item.setData(32, actor.id)
                self.actor_overview.addItem(item)

        self.map_explorer.draw_network(start_date, end_date)
        self.redrawing = False
        self.status_bar.showMessage("Finished.")

        
    #TODO: Loading additional data, causes the old actors to remain visible (edges at least)
    def reload_data(self):
        if os.path.isfile(self.actor_select.text()):
            return          
        else:
            actors = self.actor_select.text().split(",")
        self.map_explorer.gc.update_actor_list(actors)
        self.redraw()

    def change_visible_actors(self, actor):
        if actor.checkState()==Qt.Checked:
            start_date = self.start_date.dateTime().toString("yyyy-MM-dd")
            end_date = self.end_date.dateTime().toString("yyyy-MM-dd")           
            self.map_explorer.show_actor(actor.data(32),start_date,end_date)
        else:
            self.map_explorer.hide_actor(actor.data(32))

        self.map_explorer.canvas.draw()

    def display_node_details(self, node):
        
        nodeID = node.name
        self.node_detail.clear()
        content = "<b>Who was when in {} </b><br>".format(node.label)
        for actor, graph in self.map_explorer.gc.graphs():
            edges = graph.edges
            if edges.empty:
                continue            
            
            relevant_edges_from = edges[edges["from_node"]==nodeID]
            relevant_edges_to = edges[edges["to_node"]==nodeID]

            relevant_edges = edges[(edges["from_node"]==nodeID) | (edges["to_node"]==nodeID)]
            if relevant_edges.empty:
                continue

            content += "<span>{}:<span><br>".format(actor.name)
            index = relevant_edges.index.values
            if len(index)==1:
                if relevant_edges.ix[index[0]].to_node == nodeID:
                    content += "Came in {}<br>".format(relevant_edges.ix[index[0]].to_date)
                else:
                    content += "Left in {}<br>".format(relevant_edges.ix[index[0]].from_date)                     
                continue

            for i in range(len(index)-1):
                content += "{} -- {}<br>".format(relevant_edges.ix[index[i]].to_date, relevant_edges.ix[index[i+1]].from_date)
           

        self.node_detail.insertHtml(content)

    def update_status_bar(self,progress):
        if progress < 1.:
            self.status_bar.showMessage("Fetching data from db. {:.2f}% done.".format(progress*100.))
        else:
            self.status_bar.showMessage("Finished.")

    def actor_context_menu(self, pos):
        if self.actor_overview.itemAt(pos) is None:
            return
        globalPos = self.actor_overview.mapToGlobal(pos);

        context_menu = QMenu(self)
        timespan_action = QAction("Use complete timespan", self)        
        context_menu.addAction(timespan_action)
        timespan_action.triggered.connect(lambda x: self.timespan_from_actor(self.actor_overview.itemAt(pos)))

        export_action = QAction("Export graph", self)
        context_menu.addAction(export_action)
        export_action.triggered.connect(lambda x: self.export_graph(self.actor_overview.itemAt(pos)))

        remove_action = QAction("Remove actor", self)
        context_menu.addAction(remove_action)
        remove_action.triggered.connect(lambda x: self.remove_actor(self.actor_overview.itemAt(pos)))

        context_menu.exec(globalPos)

    def timespan_from_actor(self, item):
        graph = self.map_explorer.gc.get_cache_entry(item.data(32))

        sd = graph.min_date
        ed = graph.max_date
        self.start_date.setDate(QDate(sd.year, sd.month, sd.day))
        self.end_date.setDate(QDate(ed.year, ed.month, ed.day))
        self.redraw()

    def compare_selected_actors(self):
        selected_actors = [item.data(32) for item in self.actor_overview.selectedItems()]
        for actors in itertools.combinations(selected_actors, 2):
            self.map_explorer.find_cooccurence(*actors)

    def save_state(self):
        filename = QFileDialog.getSaveFileName(self, 'Save File')[0]
        if filename:
            with open(filename, "w") as f:
                f.write("Start_date,")
                f.write(self.start_date.dateTime().toString("yyyy-MM-dd"))
                f.write("\n")
                f.write("End_date,")
                f.write(self.end_date.dateTime().toString("yyyy-MM-dd"))
                f.write("\n")
                f.write("actor_id,state\n")
                for i in range(self.actor_overview.count()):
                    actor = self.actor_overview.item(i)
                    f.write("{},{}".format(actor.data(32), actor.checkState()))
                    f.write("\n")

    def load_state(self):
        filename = QFileDialog.getOpenFileName(self, 'Load File')[0]
        if filename:
            actors = []
            with open(filename, "r") as f:
                start_date = f.readline().split(",")[1]
                start_date = QDate(*[int(d) for d in start_date.split("-")])
                self.start_date.setDate(start_date)
                end_date = f.readline().split(",")[1]
                end_date = QDate(*[int(d) for d in end_date.split("-")])
                self.end_date.setDate(end_date)
                f.readline()
                for line in f.readlines():
                    actor_id, state = line.strip().split(",")
                    actors.append(actor_id)
            self.actor_overview.clear()
            self.map_explorer.gc.clear()
            self.map_explorer.gc.update_actor_list(actors)
            self.redraw()

    def import_graph(self):
        pass

    def export_graph(self, actor):
        """Export data to actorid_nodes.csv and actorid_edges.csv"""
        graph = self.map_explorer.gc.get_cache_entry(actor.data(32))
        actor = self.map_explorer.gc.get_actor(actor.data(32))
        
        if graph and actor:
            filename = QFileDialog.getSaveFileName(self, 'Save File')[0]
            if filename:
                if not filename.endswith(".tar.gz"):
                    filename = filename+".tar.gz"
                graph.to_csv(directory = "/tmp", prefix = "{}_".format(actor.id))
                actor.to_csv(directory = "/tmp")
                with tarfile.open(filename, "w:gz") as tar:
                    tar.add("/tmp/{}_nodes.csv".format(actor.id), arcname = "{}_nodes.csv".format(actor.id))
                    tar.add("/tmp/{}_edges.csv".format(actor.id), arcname = "{}_edges.csv".format(actor.id))
                    tar.add("/tmp/{}_actor.csv".format(actor.id), arcname = "{}_actor.csv".format(actor.id))



    def remove_actor(self, actor):
        if not self.redrawing:
            self.map_explorer.gc.remove_actor(actor.data(32))
            row = self.actor_overview.row(actor)
            actor = self.actor_overview.takeItem(row)
            del actor
            self.redraw()
                    
            
    
@adrastea()
def main(env):
    app = QApplication(sys.argv)    
    ex = MainWindow(env["client"])
    t = QtCore.QTimer()
    t.singleShot(0,ex.redraw)
    sys.exit( app.exec_() )

if __name__ == "__main__":    
    main()
