# EventFlow
The aim of this project is to investigate event triples (actor,location,time), while they are combined to a graph.

The contributions are: 

1. A python package 'eventflow', which enables high-level manipulation of the eventflow-graphs.
The package should be designed to be independend from a specific database (although it assumes certain fields exist).

2. Database setup script to create an eventflow graph database based on the LOAD data.

3. A visualization as an interactive qt5 app.

See docs for more informations.

## Install

python setup.py install

## Usage

Open by executing the script (or the module).

Enter a WikidataID (eg. Q1189) or a comma separated list of ids into the empty field.

Hit 'Fetch Data'

Change the start or end date and hit 'Refresh'

## TODO

Extend the eventflow package by multiple analysis capabilities.
