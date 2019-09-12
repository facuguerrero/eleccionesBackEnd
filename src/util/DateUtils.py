from datetime import datetime, timedelta, date


class DateUtils:

    @staticmethod
    def first_and_last_seconds(value):
        """ Returns two instances of datetime, one at 00:00:00 and one at 23:59:59 on the value date. """
        start = datetime.combine(value.date(), datetime.min.time())
        end = start + timedelta(days=1, seconds=-1)
        return start, end

    @staticmethod
    def is_today(value):
        """ Determine if a given date is 'today'. """
        return datetime.today().date() == value.date()

    @staticmethod
    def date_at_first_hour(value):
        """ Returns a new datetime object at 00:00:00 of given date. """
        return value.replace(hour=0, minute=0, second=0, microsecond=0)

    @staticmethod
    def date_at_last_hour(value):
        """ Returns a new datetime object at 23:59:59 of given date. """
        return value.replace(hour=23, minute=59, second=59)

    @staticmethod
    def last_second_of_day(value):
        """ Returns a new datetime object from a datetime at 00:00:00 in the same day but at 23:59:59. """
        return value + timedelta(days=1) - timedelta(seconds=1)

    @staticmethod
    def date_to_timestamp(date):
        return datetime.combine(date, datetime.min.time()).timestamp()

    @staticmethod
    def today():
        return datetime.combine(date.today(), datetime.min.time())
