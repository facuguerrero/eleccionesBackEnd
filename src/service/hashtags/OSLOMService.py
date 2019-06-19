import re

from pathlib import Path

from src.service.hashtags.HashtagCooccurrenceService import HashtagCooccurrenceService
from src.util.CommandLineUtils import CommandLineUtils
from src.util.FileUtils import FileUtils
from src.util.logging.Logger import Logger


class OSLOMService:

    RESULT_FILE_NAME = 'tp'
    SEED_FILE_NAME = 'time_seed.dat'
    RESULT_FOLDER_SUFFIX = '_oslo_files'
    OSLOM_SCRIPT_NAME = 'oslom_undir'
    OSLOM_FOLDER_NAME = 'OSLOM2'

    @classmethod
    def export_communities_for_window(cls, start_date, end_date, graph):
        """ Create .csv file with the processed result of OSLOM's execution. Add known data to graph. """
        # OSLOM2 folder path
        oslom_path = f'{Path.home()}/{cls.OSLOM_FOLDER_NAME}/'
        # OSLOM undirected script path
        script_path = f'{oslom_path}{cls.OSLOM_SCRIPT_NAME}'
        # Weights file name
        file_name = FileUtils.file_name_with_dates('weights', start_date, end_date, '.txt')
        # Weights file path
        file_path = f'{HashtagCooccurrenceService.DIR_PATH}/{file_name}'
        # Copy weights file to OSLOM2 folder
        CommandLineUtils.copy(file_path, oslom_path)
        # Execute OSLOM script on weights file
        oslom_call = f'{script_path} -f {oslom_path}{file_name} -w'
        CommandLineUtils.execute(oslom_call)
        # Move the resultant file
        CommandLineUtils.move(cls.RESULT_FILE_NAME, HashtagCooccurrenceService.DIR_PATH)
        # Remove the seed file
        CommandLineUtils.remove(cls.SEED_FILE_NAME)
        # Remove the created folder
        result_dir_path = f'{oslom_path}{file_name}{cls.RESULT_FOLDER_SUFFIX}/'
        CommandLineUtils.remove(result_dir_path)
        # Remove the copied file
        CommandLineUtils.remove(f'{oslom_path}{file_name}')
        # Process OSLOM's output
        hashtag_clusters = cls.__extract_oslom_communities()
        # Write to .csv
        cls.__write_hashtag_clusters_file(hashtag_clusters, start_date, end_date, graph)
        # Remove OSLOM result
        CommandLineUtils.remove(f'{HashtagCooccurrenceService.DIR_PATH}/{cls.RESULT_FILE_NAME}')

    @classmethod
    def __extract_oslom_communities(cls):
        """ Extract communities from OSLOM result. """
        cluster_count = 0
        hashtag_clusters = dict()
        # Read OSLOM's result and process
        with open(f'{HashtagCooccurrenceService.DIR_PATH}/{cls.RESULT_FILE_NAME}') as fd:
            cluster = ''
            for line in fd:
                # Check the type of line
                module = re.search('^#module\\s([0-9]+).*', line)
                # If this line is a module header, then we set the cluster id for the following lines
                if module is not None:
                    cluster = int(module.group(1))
                    cluster_count += 1
                # If not, then we read the hashtags
                else:
                    # Split line to get individual hashtags
                    module_hashtags = line.replace('\n', ' ').strip().split(' ')
                    # Map all hashtags to integers
                    module_hashtags = map(int, module_hashtags)
                    # Add cluster to hashtag's set of clusters in which it is included
                    for hashtag in module_hashtags:
                        if hashtag not in hashtag_clusters:
                            hashtag_clusters[hashtag] = set()
                        hashtag_clusters[hashtag].add(cluster)
        cls.get_logger().info(f'OSLOM found {cluster_count} different clusters.')
        return hashtag_clusters

    @classmethod
    def __write_hashtag_clusters_file(cls, hashtag_clusters, start_date, end_date, graph):
        """ Create .csv files with mappings for hashtag -> cluster. """
        base_dir = f'{HashtagCooccurrenceService.DIR_PATH}/'
        translator = FileUtils.file_name_with_dates(f'{base_dir}ids', start_date, end_date, '.txt')
        # Store references for mapping ids to hashtag text
        mappings = dict()
        with open(translator) as translator_fd:
            for line in translator_fd:
                splitted = line.strip().split(' ')
                mappings[int(splitted[0])] = splitted[1]
        # Create a .csv with numerical ids and one with string hashtag names
        ids = FileUtils.file_name_with_dates(f'{base_dir}ids_clusters', start_date, end_date, '.csv')
        # TODO: This may not be needed if we do not use LaNet-vi
        with open(ids, 'w') as ids_fd:
            # Write a line for each pair hashtag-cluster
            for hashtag, clusters in hashtag_clusters.items():
                for cluster in clusters:
                    graph['nodes'][mappings[hashtag]]['cluster'] = cluster
                    ids_fd.write(f'{hashtag} {cluster}\n')

    @classmethod
    def get_logger(cls):
        return Logger(cls.__name__)
