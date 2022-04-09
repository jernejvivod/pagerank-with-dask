import os
import re
import xml.etree.ElementTree as ElTree

from dask import bag as db

from . import logger


def parse_graph_xml(graph_path, graph_spec_format, id_graph=None):
    """Parse graph presented in an xml file format.

    Author: Jernej Vivod

    :param graph_path: path to the file
    :param graph_spec_format: format of the graph representation used in the file
    :param id_graph: if using ORA format, the id of the graph in the xml file
    :return: Dask Bag instance graph representation containing adjacency dictionaries mapping nodes to a set of their neighbors
    """

    logger.info('parsing graph with arguments {0}={1}, {2}={3}'.format('graph_spec_format', graph_spec_format, 'id_graph', id_graph))

    xml_root = ElTree.parse(graph_path).getroot()

    if graph_spec_format == 'ORA':

        if id_graph is None:
            raise ValueError('id of the graph should be specified when using ORA format.')

        network_to_parse = xml_root.find('.//network[@id=\'{0}\']'.format(id_graph))
        if network_to_parse is None:
            raise ValueError('network with specified id \'{0}\' not found in the provided file'.format(id_graph))

        def foldby_binop(acc, e):
            if float(e.attrib['value']) > 0.0:
                return *acc, e.attrib['target']
            else:
                return acc

        return db.from_sequence(network_to_parse).foldby(key=lambda x: x.attrib['source'], binop=foldby_binop, combine=lambda acc1, acc2: (*acc1, *acc2), initial=())
    else:
        raise NotImplementedError('specified graph format \'{0}\' not supported'.format(graph_spec_format))


def parse_graph_edgelist(graph_path, graph_spec_format):
    """Parse graph presented in an edge list format.

    Author: Jernej Vivod

    :param graph_path: path to the file
    :param graph_spec_format: format of the graph representation used in the file (for preprocessing)
    :return: Dask Bag instance graph representation containing adjacency dictionaries mapping nodes to a set of their neighbors
    """

    logger.info('parsing graph with arguments {0}={1}'.format('graph_spec_format', graph_spec_format))
    if graph_spec_format == 'LNA':

        def to_dict_item(x):
            """map line mapping node index to name to format for next step in pipeline"""
            m = re.match(r'# (.*) "(.*)"', x)
            return m.group(1), m.group(2)

        def fold_binop_f(acc, e):
            """function for binary operator for fold used when creating the dictionary mapping node indices to their names"""
            acc[e[0]] = e[1]
            return acc

        def fold_combine_f(acc1, acc2):
            """function for combine for fold used when creating the dictionary mapping node indices to their names"""
            acc1.update(acc2)
            return acc1

        # parse file contents
        file_cont = db.read_text(graph_path)

        # get dictionary mapping node indices to their names
        index_to_name = file_cont.filter(lambda x: re.match('# [0-9]+ "*', x)) \
            .map(to_dict_item) \
            .fold(binop=fold_binop_f, combine=fold_combine_f, initial=dict()) \
            .compute()

        # get graph representation
        return file_cont.filter(lambda x: re.match('[0-9]+ [0-9]+', x)) \
            .foldby(key=lambda x: index_to_name[x.split(' ')[0]], binop=lambda acc, e: (*acc, index_to_name[e.strip().split(' ')[1]]), combine=lambda acc1, acc2: (*acc1, *acc2), initial=())

    elif graph_spec_format == 'OF-routes':
        return db.read_text(graph_path) \
            .foldby(key=lambda x: x.split(',')[2], binop=lambda acc, e: (*acc, e.split(',')[4]), combine=lambda acc1, acc2: (*acc1, *acc2), initial=())


def parse_graph(graph_path, graph_spec_format=None, id_graph=None):
    """Parse graph from file and return Dask Bag instance representation.

    Author: Jernej Vivod

    :param graph_path: path to file containing the graph representation
    :param graph_spec_format: format used to specify the graph representation
    :param id_graph: if using ORA format, the id of the graph in the xml file
    :return: Dask Bag instance graph representation containing adjacency dictionaries mapping nodes to a set of their neighbors
    """

    logger.info('parsing graph from {0} with arguments {1}={2}, {3}={4}'.format(graph_path, 'graph_spec_format', graph_spec_format, 'id_graph', id_graph))

    _, file_extension = os.path.splitext(graph_path)
    if file_extension == '.xml':
        return parse_graph_xml(graph_path, graph_spec_format, id_graph)
    else:
        return parse_graph_edgelist(graph_path, graph_spec_format)
