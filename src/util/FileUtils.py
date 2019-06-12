class FileUtils:

    @staticmethod
    def file_name_with_dates(prefix, start_date, end_date, suffix=''):
        if start_date.date() == end_date.date():
            return f'{prefix}_{start_date.date()}{suffix}'
        return f'{prefix}_{start_date.date()}_{end_date.date()}{suffix}'
