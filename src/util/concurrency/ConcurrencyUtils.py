from threading import Lock
from expiringdict import ExpiringDict
from src.util.meta.Singleton import Singleton


class ConcurrencyUtils(metaclass=Singleton):

    def __init__(self):
        self.locks = ExpiringDict(max_len=100000, max_age_seconds=86400)  # Locks last a full day

    def create_lock(self, lock_id):
        """ Create a new lock if it doesn't already exist one with the given id. """
        if lock_id not in self.locks:
            self.locks[lock_id] = Lock()

    def acquire_lock(self, lock_id, block=True):
        """ Acquire lock with given id. """
        return self.locks[lock_id].acquire(block)

    def release_lock(self, lock_id):
        """ Release lock with given id. """
        self.locks[lock_id].release()
