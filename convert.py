import sys
import logging

logger = logging.basicConfig(level=logging.DEBUG)

if len(sys.argv) != 2:
    logging.error('Usage: python convert.py path_to_file')
    sys.exit(1)


class Table(object):

    def __init__(self, name, columns, constraints):
        self.name = name
        self.columns = columns
        self.constraints = constraints


class Column(object):

    def __init__(self, name, column_type, not_null=None):
        self.name = name
        self.column_type = column_type
        self.not_null = not_null


class StatementTracker(object):

    def __init__(self):
        # list of list of lines - each line in nested list is part of create
        # table statement
        self.m1_table_statements = []
        self.m1_tables = []

    def add_table(self, table):
        self.m1_table_statements.append(table)

    def parse_file(self, f):
        """ f -- file object
        """
        tracking_table = False
        current_table = []
        for idx, line in enumerate(f):
            stripped_line = line.strip().lower()
            if is_m1_table_line(stripped_line):
                tracking_table = True
                table_name = extract_m1_table_name(stripped_line)

            if tracking_table:
                current_table.append(stripped_line)

            if tracking_table and ';' in stripped_line:
                print(current_table)
                print('******')
                print(table_name)
                tracker.add_table(current_table)
                table = self.parse_table(current_table)
                if table is not None:
                   self.m1_tables.append(table)
                current_table = []
                tracking_table = False


    def parse_table(self, table):
        table_name = ""
        columns = []
        for idx, line in enumerate(table):
            if idx == 0:
                table_name = extract_m1_table_name(line)
            else:
                column = extract_column(line)
                if column is not None:
                    columns.append(column)
        return Table(table_name, columns, [])


def is_m1_table_line(line):
    split_line = line.split()
    return 'create' in split_line and \
        'table' in split_line and line.endswith('(')


def extract_column(line):
    column_type = ''

    cleaned_line = remove_unwanted_chars(line)
    split_cleaned_line = cleaned_line.split()

    if len(split_cleaned_line) == 0:
        logging.debug('Skipping empty column')
        return None

    name = split_cleaned_line[0]


    return Column(name, column_type)


def remove_unwanted_chars(line):
    """ Remove [, ], (, ), spaces and commas
    """
    return line.replace(')', '')\
        .replace('[', '')\
        .replace(']', '')\
        .replace('(', '')\
        .replace(')', '')\
        .replace(',', '')\
        .replace(';', '')\
        .strip()


def extract_m1_table_name(line):
    name = line.replace(')', '')\
        .replace('table', '')\
        .replace('create', '')\
        .replace('[', '')\
        .replace(']', '')\
        .replace('(', '')\
        .strip()
    dot_name = name.split('.')
    return dot_name[len(dot_name)-1]


filename = sys.argv[1]
with open(filename) as f:
    tracker = StatementTracker()
    tracker.parse_file(f)
logging.info('Processing done.')
