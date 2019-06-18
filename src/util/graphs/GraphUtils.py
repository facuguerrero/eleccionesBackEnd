class GraphUtils:

    @classmethod
    def create_with_weighted_edges(cls, data):
        """ Create a representation of a graph with weighted edges. """
        links = []
        nodes = dict()
        # Create weighted edges and nodes
        for key, count in data.items():
            pair = key.split('-')
            links.append({'source': pair[0],
                          'target': pair[1],
                          'weight': count})
            cls.__add_to_nodes(nodes, pair[0], count)
            cls.__add_to_nodes(nodes, pair[1], count)
        return {'links': links, 'nodes': nodes}

    @classmethod
    def unpack_nodes(cls, graph):
        """ Unpack nodes from a graph created by this same utility class also removing those nodes that do not
        belong to any cluster. """
        unpacked = {'nodes': [], 'links': []}
        for_removal = []
        # Keep only those nodes that were identified to be in a cluster
        for node in graph['nodes'].values():
            if node.get('cluster', None) is None:
                for_removal.append(node['id'])
            else:
                unpacked['nodes'].append(node)
        # Keep only those links connecting nodes that belong to a cluster
        for link in graph['links']:
            if link['source'] not in for_removal and link['target'] not in for_removal:
                unpacked['links'].append(link)
        return unpacked

    @classmethod
    def __add_to_nodes(cls, nodes, node, edge_weight):
        """ Add to nested node dictionary. Has id repeated because it will be unpacked later. """
        if node not in nodes:
            nodes[node] = {'id': node, 'size': 0}
        nodes[node]['size'] += edge_weight + 1  # One to count the edge we are currently reading
