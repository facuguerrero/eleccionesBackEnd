from threading import Lock

from src.util.meta.Singleton import Singleton


class ConcurrencyUtils(metaclass=Singleton):

    def __init__(self):
        self.locks = {}

    def create_lock(self, lock_id):
        """ Create a new lock if it doesn't already exist one with the given id. """
        if lock_id not in self.locks:
            self.locks[lock_id] = Lock()

    def acquire_lock(self, lock_id):
        """ Acquire lock with given id. """
        self.locks[lock_id].acquire(True)

    def release_lock(self, lock_id):
        """ Release lock with given id. """
        self.locks[lock_id].release()
