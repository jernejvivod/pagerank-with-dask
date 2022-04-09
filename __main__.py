import argparse
import os.path
import sys

import tabulate

from pagerank_with_dask.graph_parsing import parse_graph
from pagerank_with_dask.page_rank import page_rank
from pagerank_with_dask.visualization import visualize


def main(graph_path, graph_spec_format, id_graph, n_iter, initial_pr, damping_factor, n_top_print, plot=False, plot_graph_heatmap=False, n_top=7, plot_path='.'):
    """entry point for the project. Compute and present PageRank scores for a specified graph with specified parameters.

    See README.md in the project root directory for more information.

    Author: Jernej Vivod

    :param graph_path: path to the file containing the graph representation for which to compute and present the PageRank scores
    :param graph_spec_format: format used to specify the graph representation
    :param id_graph: if using ORA format, the id of the graph in the xml file
    :param n_iter: number of iterations of the PageRank algorithm to perform
    :param initial_pr: initial PageRank score of each node
    :param damping_factor: damping factor value
    :param n_top_print: how many nodes and their PageRank scores to display when printing the results
    :param plot: plot the results or not
    :param plot_graph_heatmap: plot the graph PageRank scores heatmap visualization or not
    :param n_top: number of top nodes by PageRank scores to include in the plot
    :param plot_path: path to the directory for storing the result plots
    """

    # parse graph
    graph = parse_graph.parse_graph(graph_path, graph_spec_format, id_graph)

    # compute PageRank scores
    pr_scores = page_rank.pagerank(graph, n_iter, initial_pr=initial_pr, damping_factor=damping_factor)

    # print results formatted as a table
    print('Top {0} nodes by their PageRank scores:'.format(min(len(pr_scores), n_top_print)))
    print(tabulate.tabulate([('Node Name', 'PageRank Score')] + sorted(pr_scores.items(), key=lambda x: x[1], reverse=True)[:n_top_print], headers='firstrow'))

    if plot:
        visualize.plot_top_pr_scores(pr_scores=pr_scores, plot_path=plot_path, n_top=n_top)
        if plot_graph_heatmap:
            visualize.plot_graph_heatmap(graph=graph, pr_scores=pr_scores, plot_path=plot_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='pagerank-with-dask')

    # graph parsing
    parser.add_argument('graph_path', type=str, help='path to the file containing the graph specification')
    parser.add_argument('graph_spec_format', type=str, choices=['ORA', 'OF-routes', 'LNA'], help='format in which the graph is specified')
    parser.add_argument('--id-graph', type=str, help='id of the graph to parse if using ORA format')

    # PageRank parameters
    parser.add_argument('--n-iterations', type=int, default=20, help='number of PageRank algorithm iterations to perform')
    parser.add_argument('--initial-pr', type=float, default=0.25, help='initial PageRank score of each node')
    parser.add_argument('--damping-factor', type=float, default=0.85, help='damping factor value')

    # visualization
    parser.add_argument('--n-top-print', type=int, default=20, help='how many nodes and their PageRank scores to display when printing the results')
    parser.add_argument('--plot', action='store_true', help='plot the results')
    parser.add_argument('--plot-heatmap', action='store_true', help='plot the graph PageRank scores heatmap visualization')
    parser.add_argument('--n-top', type=int, default=7, help='number of top nodes by PageRank scores to include in the plot')
    parser.add_argument('--plot-path', type=str, help='path to the directory for storing the result plots', required='--plot' in sys.argv)

    args = parser.parse_args()
    if args.n_top_print < 1 or (args.plot and args.n_top < 1):
        raise argparse.ArgumentTypeError('Value of argument specifying the number of nodes and their PageRank scores to include in the results should be greater than 0.')

    if args.plot and not os.path.isfile(args.graph_path):
        parser.error('File {0} does not exist.'.format(args.graph_path))

    if args.plot and not os.path.isdir(args.plot_path):
        parser.error('{0} is not a directory.'.format(args.plot_path))

    if args.plot:
        main(
            graph_path=args.graph_path,
            graph_spec_format=args.graph_spec_format,
            id_graph=args.id_graph,
            n_iter=args.n_iterations,
            initial_pr=args.initial_pr,
            damping_factor=args.damping_factor,
            n_top_print=args.n_top,
            plot=True,
            plot_graph_heatmap=args.plot_heatmap,
            n_top=args.n_top,
            plot_path=args.plot_path)
    else:
        main(
            graph_path=args.graph_path,
            graph_spec_format=args.graph_spec_format,
            id_graph=args.id_graph,
            n_iter=args.n_iterations,
            initial_pr=args.initial_pr,
            damping_factor=args.damping_factor,
            n_top_print=args.n_top_print)
