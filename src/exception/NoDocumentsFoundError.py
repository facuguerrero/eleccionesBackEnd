class NoDocumentsFoundError(Exception):

    def __init__(self, collection_name, query):
        self.message = f'No documents found on collection {collection_name} with query {query}.'

    def __str__(self):
        return self.message
