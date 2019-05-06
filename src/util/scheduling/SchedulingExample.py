class SchedulingExample:

    COUNT = 0

    @classmethod
    def count(cls):
        cls.COUNT += 1
        return cls.COUNT
