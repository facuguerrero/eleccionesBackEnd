class InterleavedQueue:
    """ Implementation of queue that makes sure that, whenever possible, no two elements of same `type` are
    next to each other. Said `type` is a field defined by the user. """

    def __init__(self, lists_by_key: dict):
        self.queue = list()
        # Keep only those key-value pairs where the list is not empty
        lists_by_key = {k: v for k, v in lists_by_key.items() if v}
        # Populate queue
        keys = list(lists_by_key.keys())
        lists = list(lists_by_key.values())
        # Get an element from each list round-robin style and remove lists that are depleted
        iterator = 0
        while lists:
            index = iterator % len(lists)
            element = lists[index].pop(0)
            # Queue elements will have their key associated
            self.queue.append(self.Item(keys[index], element))
            if not lists[index]:
                del lists[index]
                del keys[index]
            iterator += 1

    class Item:
        def __init__(self, key, data):
            self.key = key
            self.data = data

    def pop(self):
        try:
            value = self.queue.pop(0)
        except IndexError:
            value = None
        return value

    def __len__(self):
        return len(self.queue)