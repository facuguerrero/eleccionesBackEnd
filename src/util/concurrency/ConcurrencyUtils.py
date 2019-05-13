from threading import Semaphore

from src.util.meta.Singleton import Singleton


class ConcurrencyUtils(metaclass=Singleton):

    def __init__(self):
        self.locks = {}

    def create_lock(self, lock_id):
        self.locks[lock_id] = Semaphore()

    def acquire_lock(self, lock_id):
        self.locks[lock_id].acquire(True)

    def release_lock(self, lock_id):
        self.locks[lock_id].release()
