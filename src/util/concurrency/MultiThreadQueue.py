import uuid

from src.util.concurrency.ConcurrencyUtils import ConcurrencyUtils


class MultiThreadQueue:

    def __init__(self):
        self.id = str(uuid.uuid4())
        self.queue = list()
        # The lock is not really needed. Python's data structures are thread safe because of the GIL
        ConcurrencyUtils().create_lock(self.id)

    def put(self, element):
        ConcurrencyUtils().acquire_lock(self.id)
        self.queue.append(element)
        ConcurrencyUtils().release_lock(self.id)

    def put_many(self, elements: list):
        ConcurrencyUtils().acquire_lock(self.id)
        self.queue += elements
        ConcurrencyUtils().release_lock(self.id)

    def pop(self):
        ConcurrencyUtils().acquire_lock(self.id)
        self.queue.pop(0)
        ConcurrencyUtils().release_lock(self.id)
