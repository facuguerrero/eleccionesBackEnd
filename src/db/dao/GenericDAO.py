from pymongo import ReturnDocument


class GenericDAO:

    def __init__(self, collection):
        self.collection = collection

    def create_indexes(self):
        # Subclass responsibility
        pass

    def get_first(self, query, projection_dict=None):
        """
        Get first entry matching the given query.
            :returns Full document
        """
        return self.collection.find_one(query, projection_dict)

    def get_all(self, query=None, projection_dict=None):
        """
        Get all entries matching the given query. If there is no query, full collection is returned.
            :returns List of full documents
        """
        return self.collection.find({} if query is None else query, projection_dict)

    def get_count(self, query=None, projection_dict=None):
        """
        Get all entries matching the given query. If there is no query, full collection is returned.
            :returns List of full documents
        """
        return self.collection.find({} if query is None else query, projection_dict).count()

    def get_with_limit(self, query=None, projection_dict=None, limit=24000):
        """
        Get entries matching the given query.
        The maximum quantity of results are 5000
        """
        return self.collection.find({} if query is None else query, projection_dict).limit(limit)

    def get_with_cursor(self, query=None, projection_dict=None, sort=None, skip=0, limit=0):
        """
         Get all entries matching the given query using a cursor formed by skip and limit parameters.
         If there is no query, full collection is returned. Using no limit results in the same behaviour as get_all.
         Using no sort conditions could result in an unexpected behaviour of the cursor
            :returns List of full documents
        """
        return self.get_all(query, projection_dict).sort({} if sort is None else sort).skip(skip).limit(limit)

    def insert(self, element):
        """
        Insert given element into collection.
            :returns An instance of InsertOneResult (ior.inserted_id gives the created id)
        """
        return self.collection.insert_one(element)

    def delete_first(self, query):
        """
        Delete first element matching the given query from collection.
            :returns An instance of DeleteResult (dr.delete_count returns the amount of deleted documents)
        """
        return self.collection.find_one_and_delete(query)

    def delete_all(self, query=None):
        """
        Delete all entries matching the given query. If there is no query, collection is cleared.
            :returns An instance of DeleteResult (dr.delete_count returns the amount of deleted documents)
        """
        return self.collection.delete_many({} if query is None else query)

    def update_first(self, query, updated_fields_dict):
        """
        Update first entry matching given query with the given dictionary.
            :returns Updated document
        """
        return self.collection.find_one_and_update(filter=query,
                                                   update={'$set': updated_fields_dict},
                                                   return_document=ReturnDocument.AFTER)

    def remove_fields_first(self, query, removed_fields_dict):
        """
        Add given fields to first entry matching given query.
            :returns Updated document
        """
        return self.collection.find_one_and_update(filter=query,
                                                   update={'$unset': removed_fields_dict},
                                                   return_document=ReturnDocument.AFTER)

    def remove_document(self, query):
        """
        Deletes document matching given entry
        """
        self.collection.delete_one(query)

    def upsert(self, query, update_dict):
        """
        Creates entry if it doesn't exists and updates it if it does.
            :returns Updated document
        """
        return self.collection.find_one_and_update(filter=query,
                                                   update=update_dict,
                                                   upsert=True,
                                                   return_document=ReturnDocument.AFTER)

    def aggregate(self, stages):
        """
        Aggregate documents by given stages
        """
        return self.collection.aggregate(stages, allowDiskUse=True, maxTimeMS=900000)
