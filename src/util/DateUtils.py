from datetime import datetime


class DateUtils:

    @staticmethod
    def is_today(value):
        """ Determine if a given date is 'today'. """
        return datetime.today().date() == value.date()
