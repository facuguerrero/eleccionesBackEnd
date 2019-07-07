from datetime import datetime, timedelta


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
    def last_second_of_day(value):
        """ Returns a new datetime object from a datetime at 00:00:00 in the same day but at 23:59:59. """
        return value + timedelta(days=1) - timedelta(seconds=1)

    @staticmethod
    def date_to_timestamp(date):
        return datetime.combine(date, datetime.min.time()).timestamp()
