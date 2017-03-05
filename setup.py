from distutils.core import setup

setup(name='EventFlow',
      version='1.0',
      description='Explorative tool for the flows of wikidata events',
      author='Jan Greulich',
      packages=['eventflow'],
      package_data={'eventflow':["config.ini"]},
      install_requires=['python>=3.6.0',
            'numpy>=1.11.3',
            'pandas>=0.19.2',
            'geopandas',
            'shapely>=1.5.17',
            'matplotlib>=2.0.0',
            'pymongo>=3.3.0',
            'sshtunnel==0.1.2']
     )
