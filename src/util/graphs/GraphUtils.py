from itertools import islice
from random import sample

import pandas as pd

from src.service.hashtags.HashtagCooccurrenceService import HashtagCooccurrenceService
from src.util.FileUtils import FileUtils
from src.util.config.ConfigurationManager import ConfigurationManager


class GraphUtils:

    @classmethod
    def create_cooccurrence_graphs(cls, start_date, end_date):
        """ Generate all cooccurrence graphs related to a particular date. """
        base_dir = f'{HashtagCooccurrenceService.DIR_PATH}/'
        # Weights file path
        weights_path = FileUtils.file_name_with_dates(f'{base_dir}weights', start_date, end_date, '.txt')
        # Hashtag by cluster file path
        ids_path = FileUtils.file_name_with_dates(f'{base_dir}ids_clusters', start_date, end_date, '.csv')
        # Read weights file as a graph
        graph = pd.read_csv(weights_path, header=None, names=['hashtag_id_1', 'hashtag_id_2', 'weight'], sep=' ')
        # Store to which communities belong each node
        nodes_communities = pd.read_csv(ids_path, header=None, names=['hashtag_id', 'community_id'], sep=' ')
        # Join data frames to find connections between nodes and communities
        joined = graph.merge(nodes_communities, left_on='hashtag_id_1', right_on='hashtag_id')
        joined = joined.merge(nodes_communities, left_on='hashtag_id_2', right_on='hashtag_id')
        # Keep only those fields in which both nodes belong to the same community
        reduced_join = joined[['hashtag_id_1', 'hashtag_id_2', 'weight', 'community_id_x', 'community_id_y']]
        communities_graph = reduced_join[reduced_join.community_id_x == reduced_join.community_id_y]
        communities_graph = communities_graph[['hashtag_id_1', 'hashtag_id_2', 'weight', 'community_id_x']]
        communities_graph.columns = ['hashtag_id_1', 'hashtag_id_2', 'weight', 'community_id']
        # Group by community to generate said community's graph
        grouped = communities_graph.groupby('community_id')
        # Map to relate each hashtag to all the topics it belongs to
        hashtags_topics = dict()
        # Get translation map
        mappings = cls.__generate_translation_map(base_dir, start_date, end_date)
        # Generate all community graphs
        graphs = cls.__generate_community_graph(grouped, mappings, hashtags_topics)
        # Get each community's identifying hashtag
        community_leaders = cls.__find_community_leaders(graphs)
        # Create community strength data frame
        community_strength = cls.__calculate_strengths(grouped)
        # Generate main graph (community connection graph)
        graphs['main'] = cls.__generate_main_graph(reduced_join, community_leaders)
        # Generate showable graphs. These are the graphs from the main topics with a limited number of nodes
        showable_graphs = cls.__generate_showable_graphs(graphs)
        # Return data object
        return {'graphs': graphs,
                'community_strength': community_strength,
                'hashtags_topics': hashtags_topics,
                'showable_graphs': showable_graphs}

    @classmethod
    def __generate_showable_graphs(cls, graphs):
        """ Generate graphs from the main topics with a limited number of nodes. """
        bound = ConfigurationManager().get_int('max_nodes_showable_graphs')
        showable_graphs = {'main': graphs.pop('main')}
        # Find main topics
        main_topics = [node['id'] for node in showable_graphs['main']['nodes']]
        # Create a graph for each topic
        for main_topic in main_topics:
            graph = graphs[main_topic]
            # Find the 'bound' top nodes in current topic
            top_nodes = [node for node in islice(sorted(graph['nodes'], key=lambda n: n['size'], reverse=True), bound)]
            nodes_ids = [node['id'] for node in top_nodes]
            # Keep only the edges between the top nodes
            links = [link for link in graph['links'] if link['source'] in nodes_ids and link['target'] in nodes_ids]
            # Keep only an specific number of links
            links = cls.__filter_links(links, nodes_ids)
            # Store in showable graphs dict
            showable_graphs[main_topic] = {'links': links, 'nodes': top_nodes}
        return showable_graphs

    @classmethod
    def __generate_main_graph(cls, communities, community_leaders):
        """ Create a graph where each node is a community and each link indicates the
        connection between two communities"""
        main_communities = cls.__find_main_communities(communities)
        community_links = cls.__calculate_cross_community_links(communities, main_communities)
        # Create graph with given data
        links = []
        nodes = dict()
        for key, weight in community_links.items():
            cluster1, cluster2 = key.split('-')
            links.append({'source': cluster1, 'target': cluster2, 'weight': int(weight)})
            # Create a node for each hashtag
            cls.__add_to_nodes(nodes, cluster1, main_communities[cluster1], community_leaders, add=False)
            cls.__add_to_nodes(nodes, cluster2, main_communities[cluster2], community_leaders, add=False)
        # Keep only an specific number of links
        links = cls.__filter_links(links, nodes.keys())
        return {'links': links, 'nodes': list(nodes.values())}

    @classmethod
    def __find_main_communities(cls, communities):
        """ Find most main topics. """
        # Keep only links between nodes inside same topic
        data_frame = communities[communities.community_id_x == communities.community_id_y]
        # Leave only link weight column and community id
        data_frame = data_frame[['weight', 'community_id_x']]
        data_frame.columns = ['w', 'c']
        # Group by community, sum weights and sort values.
        data_frame = data_frame.groupby('c').sum().sort_values('w', ascending=False)
        # Find main nodes
        nodes = dict()
        bound = ConfigurationManager().get_int('main_communities_count')
        # Iterate through rows limiting the number of iterations with bound
        for community, data in islice(data_frame.iterrows(), 0, bound):
            weight = data['w']
            nodes[str(community)] = int(weight)
        return nodes

    @classmethod
    def __generate_community_graph(cls, groups, mappings, hashtags_topics):
        """ Create a graph for each community. """
        graphs = dict()
        # Iterate through all communities
        for name, group in groups:
            links = []
            nodes = dict()
            # Iterate through all links
            # TODO: Write .csv files?
            for _, row in group.iterrows():
                hashtag_1 = mappings[str(row['hashtag_id_1'])]
                hashtag_2 = mappings[str(row['hashtag_id_2'])]
                cls.__add_to_graph(links, nodes, hashtag_1, hashtag_2, int(row['weight']))
                cls.__append_to_hashtag_topics(hashtags_topics, hashtag_1, str(name))
                cls.__append_to_hashtag_topics(hashtags_topics, hashtag_2, str(name))
            # Store community's graph
            graphs[str(name)] = {'links': links, 'nodes': list(nodes.values())}
        # Return holder
        return graphs

    @classmethod
    def __calculate_cross_community_links(cls, communities, main_communities):
        """ Generate a dictionary with community links and their weights. """
        communities.columns = ['h1', 'h2', 'w', 'c1', 'c2']
        # Keep only those entries where communities and hashtags are different
        communities = communities[(communities.c1 != communities.c2) & (communities.h1 != communities.h2)]
        # Keep only those entries where community 1 and community 2 are main communities
        communities = communities[(communities.c1.isin(main_communities.keys()) &
                                   communities.c2.isin(main_communities.keys()))]
        verifier = set()
        counts = dict()
        # Iterate through all entries
        for _, row in communities.iterrows():
            c1, c2 = str(row['c1']), str(row['c2'])
            # Process link
            sorted_hashtags = '-'.join(sorted([str(row['h1']), str(row['h2'])]))
            sorted_communities = '-'.join(sorted([c1, c2]))
            key = '%s-%s' % (sorted_hashtags, sorted_communities)
            # Consider only those that are not repeating the 4-tuple hashtag1, hashtag2, cluster1 and cluster2
            if key not in verifier:
                verifier.add(key)
                # Add the current weight to the link
                if sorted_communities not in counts:
                    counts[sorted_communities] = 0
                counts[sorted_communities] += int(row['w'])
        # Keep only those links big enough to be shown
        return {str(k): v for k, v in counts.items()}

    @classmethod
    def __generate_translation_map(cls, base_dir, start_date, end_date):
        """ Create a dictionary to map between numeric and text ids for hashtags. """
        translator = FileUtils.file_name_with_dates(f'{base_dir}ids', start_date, end_date, '.txt')
        # Store references for mapping ids to hashtag text
        mappings = dict()
        with open(translator) as translator_fd:
            for line in translator_fd:
                splitted = line.strip().split(' ')
                mappings[splitted[0]] = splitted[1]
        return mappings

    @classmethod
    def __find_community_leaders(cls, community_graphs):
        """ Find the most important node of each community. For most important we refer to the one with the highest
        grade in the graph. """
        number_of_nodes = ConfigurationManager().get_int('representing_nodes')
        leaders = dict()
        for community_id, graph in community_graphs.items():
            # Get the 3 biggest nodes
            sorted_nodes = sorted(graph['nodes'], key=lambda node: node['size'], reverse=True)[:number_of_nodes]
            # Get the ids of these 3 hashtags
            sorted_nodes = list(map(lambda node: str(node['id']), sorted_nodes))
            # Store the set in the community
            leaders[community_id] = sorted_nodes
        return leaders

    @classmethod
    def __calculate_strengths(cls, groups):
        """ Returns a dict mapping each community to its strength. """
        return {str(name): sum(group.weight) for name, group in groups}

    @classmethod
    def __filter_links(cls, links, nodes_ids):
        """ Keep only a number N of links. Keeping at least one link per node. """
        max_links = ConfigurationManager().get_int('max_edges_showable_graphs')
        links_copy = sorted(links, key=lambda l: l['weight'], reverse=True)
        used_nodes = set()
        result = list()
        added_links = 0
        # Get a link for each node
        for node_id in nodes_ids:
            # Only search if this node was not yet "touched"
            if node_id not in used_nodes:
                # Get a link connected to the current node
                link = next(filter(lambda l: l['source'] == node_id or l['target'] == node_id, links_copy))
                # Mark the currently used nodes
                used_nodes.add(link['source'])
                used_nodes.add(link['target'])
                # Remove the link from the list and append to result
                links_copy.remove(link)
                result.append(link)
                added_links += 1
        # Get first N links. The number of links will be the minimum between
        # the configurable value and the remaining links
        random_links = links_copy[:min(max_links-added_links, len(links_copy))]
        # Return the sum of the first links and the sample
        return result + random_links

    @classmethod
    def __add_to_graph(cls, links, nodes, node1, node2, weight, mapper=None):
        """ Use given link information to add to given graph. """
        links.append({'source': node1, 'target': node2, 'weight': weight})
        # Create a node for each hashtag
        cls.__add_to_nodes(nodes, node1, weight, mapper)
        cls.__add_to_nodes(nodes, node2, weight, mapper)

    @classmethod
    def __add_to_nodes(cls, nodes, node, edge_weight, mapper, add=True):
        """ Add to nested node dictionary. Has id repeated because it will be unpacked later. """
        if node not in nodes:
            nodes[node] = {'id': node, 'size': 0}
        if mapper and 'representation' not in nodes[node]:
            nodes[node]['representation'] = mapper[node]
        if add:
            nodes[node]['size'] += edge_weight
        else:
            nodes[node]['size'] = edge_weight

    @classmethod
    def __append_to_hashtag_topics(cls, hashtags_topics, hashtag, topic):
        """ Append to list of topics related to the given hashtag. """
        if hashtag not in hashtags_topics:
            hashtags_topics[hashtag] = set()
        hashtags_topics[hashtag].add(topic)
