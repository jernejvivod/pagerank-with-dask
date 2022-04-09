from setuptools import setup

setup(
    name='pagerank-with-dask',
    version='0.1.0',
    packages=['pagerank_with_dask', 'pagerank_with_dask.page_rank', 'pagerank_with_dask.graph_parsing'],
    url='https://github.com/jernejvivod/pagerank-with-dask',
    license='',
    author='Jernej Vivod',
    author_email='vivod.jernej@gmail.com',
    description='PageRank implementation with Dask'
)
