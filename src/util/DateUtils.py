from datetime import datetime


class DateUtils:

    @staticmethod
    def is_today(value):
        return datetime.today().date() == value.date()
