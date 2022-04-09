from . import logger


def pagerank(graph, n_iter=20, initial_pr=0.1, damping_factor=0.85):
    """PageRank algorithm implementation using Dask.

    Compute PageRank scores for the graph represented as a Dask Bag containing
    adjacency dictionaries mapping nodes to a set of their neighbors.

    Author: Jernej Vivod

    :param graph: graph represented as a Dask bag of dictionaries mapping nodes to a set of their neighbors
    :param n_iter: number of iterations of the PageRank algorithm to perform
    :param initial_pr: initial PageRank score of each node
    :param damping_factor: damping factor value
    :return: dictionary mapping nodes to their computed PageRank scores
    """

    logger.info('performing PageRank algorithm ({0} iterations)'.format(n_iter))

    # count nodes for damping
    n_nodes = graph.count().compute()

    ranks = graph.map(lambda x: (x[0], initial_pr))
    for i in range(n_iter):
        joined = graph.join(ranks, lambda x: x[0]).repartition(npartitions=100)
        ranks = joined.map(lambda x: [(e, x[0][1] / len(x[1][1])) for e in x[1][1]]) \
            .flatten() \
            .foldby(key=lambda x: x[0], binop=lambda acc, e: acc + e[1], combine=lambda acc1, acc2: acc1 + acc2, initial=0) \
            .map(lambda x: (x[0], (1 - damping_factor) / n_nodes + damping_factor * x[1]))

    return dict(ranks.compute())
