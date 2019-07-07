from datetime import datetime, timedelta


class DateUtils:

    @staticmethod
    def is_today(value):
        """ Determine if a given date is 'today'. """
        return datetime.today().date() == value.date()

    @staticmethod
    def last_second_of_day(value):
        """ Returns a new datetime object from a datetime at 00:00:00 in the same day but at 23:59:59. """
        return value + timedelta(days=1) - timedelta(seconds=1)

    @staticmethod
    def date_to_timestamp(date):
        return datetime.combine(date, datetime.min.time()).timestamp()
