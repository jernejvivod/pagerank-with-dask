import os

import matplotlib.pyplot as plt
import networkx as nx

from . import logger


def plot_top_pr_scores(pr_scores, plot_path, n_top=7):
    """Plot top PageRank scores with a bar plot.

    Author: Jernej Vivod

    :param pr_scores: dictionary mapping node names to PageRank scores
    :param plot_path: path to folder for plots
    :param n_top: number of top nodes by PageRank scores to include in the plot
    """

    logger.info('plotting a bar plot of the PageRank scores of top ranked {0} nodes.'.format(n_top))

    node_names, pr_scores = zip(*sorted(pr_scores.items(), key=lambda x: x[1], reverse=True))
    plt.bar(node_names[:n_top], pr_scores[:n_top])
    plt.xticks(rotation=-20)
    plt.ylabel('PageRank Score')
    plt.savefig(os.path.join(plot_path, 'pr_scores_bar.svg'))
    plt.clf()


def plot_graph_heatmap(graph, pr_scores, plot_path):
    """plot graph with heatmap node colors reflecting the computed PageRank scores.

    :param graph: graph represented as a Dask bag of dictionaries mapping nodes to a set of their neighbors
    :param pr_scores: dictionary mapping node names to PageRank scores
    :param plot_path: path to folder for plots
    """

    logger.info('ploting the graph PageRank scores heatmap visualization')

    def to_heatmap_val(minimum, maximum, value):
        minimum, maximum = float(minimum), float(maximum)
        ratio = 2 * (value - minimum) / (maximum - minimum)
        b = int(max(0, 255 * (1 - ratio)))
        r = int(max(0, 255 * (ratio - 1)))
        g = 255 - b - r
        return r / 255, g / 255, b / 255

    pr_min, pr_max = min(pr_scores.values()), max(pr_scores.values())

    def graph_const_binop(g, e):
        for n in e[1]:
            g.add_edge(e[0], n)
        return g

    graph_nx = graph.fold(binop=graph_const_binop, combine=lambda g1, g2: nx.compose(g1, g2), initial=nx.DiGraph()).compute()
    nx.draw(graph_nx, with_labels=True, node_color=[to_heatmap_val(pr_min, pr_max, pr_scores[node]) for node in graph_nx.nodes()])
    plt.savefig(os.path.join(plot_path, 'graph_heatmap.svg'))
    plt.clf()
