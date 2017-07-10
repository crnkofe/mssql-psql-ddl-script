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

    def as_create_table(self):
        statements = ["CREATE TABLE {name} IF NOT EXISTS (".format(name=self.name)]
        column_statements = []
        for column in self.columns:
            column_statements.append("\t{col}".format(col=column.as_create_table_column()))
        statements.append(',\n'.join(column_statements))
        statements.append(');')
        return '\n'.join(statements)

    def as_create_constraints(self):
        return ""


class Column(object):

    def __init__(self, name, column_type, column_type_extension=None, not_null=None):
        self.name = name
        self.column_type = column_type
        self.column_type_extension = column_type_extension
        self.not_null = not_null

    def as_create_table_column(self):
        """ TODO: assume source dialect as mssql and target as psql
        """
        modifiers = ""
        if self.not_null is not None:
            if self.not_null:
                modifiers += 'NOT NULL'
            else:
                modifiers += 'NULL'

        return "{name} {column_type} {modifiers}".format(
            name=self.name,
            column_type=self.map_type(self.column_type),
            modifiers=modifiers
        )

    def map_type(self, from_type):
        """
        1	BIGINT	64-bit integer	BIGINT
        2	BINARY(n)	Fixed-length byte string	BYTEA
        3	BIT	1, 0 or NULL	BOOLEAN
        4	CHAR(n), CHARACTER(n)	Fixed-length character string,	CHAR(n), CHARACTER(n)
        5	DATE	Date (year, month and day)	DATE
        6	DATETIME	Date and time with fraction	TIMESTAMP(3)
        7	DATETIME2(p)	Date and time with fraction	TIMESTAMP(p)
        8	DATETIMEOFFSET(p)	Date and time with fraction and time zone	TIMESTAMP(p) WITH TIME ZONE
        9	DECIMAL(p,s), DEC(p,s)	Fixed-point number	DECIMAL(p,s), DEC(p,s)
        10	DOUBLE PRECISION	Double-precision floating-point number	DOUBLE PRECISION
        11	FLOAT(p)	Floating-point number	DOUBLE PRECISION
        12	IMAGE	Variable-length binary data, 2G	BYTEA
        13	INT, INTEGER	32-bit integer	INT, INTEGER
        14	MONEY	64-bit currency amount	MONEY
        15	NCHAR(n)	Fixed-length Unicode UCS-2 string	CHAR(n)
        16	NTEXT	Variable-length Unicode UCS-2 data, 2G 	TEXT
        17	NUMERIC(p,s)	Fixed-point number	NUMERIC(p,s)
        18	NVARCHAR(n)	Variable-length Unicode UCS-2 string	VARCHAR(n)
        19	NVARCHAR(max)	Variable-length Unicode UCS-2 data, 2G 	TEXT
        20	REAL	Single-precision floating-point number	REAL
        21	ROWVERSION	Automatically updated binary data 	BYTEA
        22	SMALLDATETIME	Date and time	TIMESTAMP(0)
        23	SMALLINT	16-bit integer	SMALLINT
        24	SMALLMONEY	32-bit currency amount	MONEY
        25	TEXT	Variable-length character data, 2G 	TEXT
        26	TIME(p)	Time (hour, minute, second and fraction)	TIME(p)
        27	TIMESTAMP	Automatically updated binary data 	BYTEA
        28	TINYINT	8-bit unsigned integer, 0 to 255 	SMALLINT
        29	UNIQUEIDENTIFIER	16-byte GUID (UUID) data 	CHAR(16)
        30	VARBINARY(n)	Variable-length byte string, 1 8000	BYTEA
        31	VARBINARY(max)	Variable-length binary data, 2G	BYTEA
        32	VARCHAR(n)	Variable-length character string, 1 8000	VARCHAR(n)
        33	VARCHAR(max)	Variable-length character data, 2G 	TEXT
        34	XML	XML data	XML
        """
        simple_type_map = {
            'bigint': 'bigint',
            'binary': 'bytea',
            'bit': 'boolean',
            'date': 'date',
            'datetime': 'timestamp(3)',
            'double precision': 'double precision',
            'float': 'double precision',
            'real': 'double precision',
            'image': 'bytea',
            'int': 'int',
            'integer': 'integer',
            'money': 'money',
            'ntext': 'text',
            'real': 'real',
            'rowversion': 'bytea',
            'smalldatetime': 'timestamp(0)',
            'smallint': 'smallint',
            'smallmoney': 'money',
            'text': 'text',
            'timestamp': 'bytea',
            'tinyint': 'smallint',
            'uniqueidentifier': 'char(16)',
            'varbinary': 'bytea',
            'xml': 'xml',
        }

        compound_type_map = {
            'char': lambda x: 'char({n})'.format(n=x),
            'character': lambda x: 'character({n})'.format(n=x),
            'varchar': lambda x: 'varchar({n})'.format(n=x),
            'nvarchar': lambda x: 'varchar({n})'.format(n=x),
            'time': lambda x: 'time({n})'.format(n=x),
            'datetime2': lambda x: 'timestamp({n})'.format(n=x or 3),
            'datetimeoffset': lambda x: 'timestamp({n})'.format(n=x or 3),
            'dec': lambda x: 'dec({m}, {n})'.format(m=x[0], n=x[1])  if x is not None else 'dec',
            'numeric': lambda x: 'decimal({m})'.format(m=x)  if x is not None else 'decimal',
            'decimal': lambda x: 'decimal({m}, {n})'.format(m=x[0], n=x[1]) if x is not None else 'decimal',
        }
        #'char': 'char'
        #'datetime2': 'timestamcp
        #'decimal':
        mapped_simple_type = simple_type_map.get(from_type, None)
        if mapped_simple_type is not None:
            return mapped_simple_type

        compound_func = compound_type_map.get(from_type, None)
        if compound_func is not None:
            return compound_func(self.column_type_extension)
        return 'unknown({})'.format(from_type)


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

            if tracking_table:
                current_table.append(stripped_line)

            if tracking_table and ';' in stripped_line:
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
            elif idx == len(table)-1:
                continue
            else:
                column = extract_column(line)
                if column is not None:
                    columns.append(column)
        return Table(table_name, columns, [])

    def as_psql(self):
        ret = []
        for table in self.m1_tables:
            ret.append(table.as_create_table())
        for table in self.m1_tables:
            ret.append(table.as_create_constraints())
        return '\n'.join(ret)


def is_m1_table_line(line):
    split_line = line.split()
    return 'create' in split_line and \
        'table' in split_line # and line.endswith('(')


def extract_column(line):
    cleaned_line = remove_unwanted_chars(line)
    split_cleaned_line = [y for y in [x.strip() for x in cleaned_line.split()] if not len(y) == 0]
    cleaned_line = ' '.join(split_cleaned_line)

    if 'constraint' in split_cleaned_line:
        return None

    if len(split_cleaned_line) == 0:
        logging.debug('Skipping empty column')
        return None

    name = split_cleaned_line[0]
    column_type = split_cleaned_line[1]

    extension = None
    if len(split_cleaned_line) >= 3:
        if split_cleaned_line[2].isdigit():
            extension = int(split_cleaned_line[2])

    if len(split_cleaned_line) >= 4:
        if split_cleaned_line[2].isdigit() and split_cleaned_line[3].isdigit():
            extension = (int(split_cleaned_line[2]), int(split_cleaned_line[3]))

    not_null = None
    if "not" in split_cleaned_line and "null" in split_cleaned_line:
        not_null = True
    elif "null" in split_cleaned_line:
        not_null = False

    return Column(name, column_type, column_type_extension=extension, not_null=not_null)


def remove_unwanted_chars(line):
    """ Remove [, ], (, ), spaces and commas
    """
    return line.replace(')', '')\
        .replace('[', ' ')\
        .replace(']', ' ')\
        .replace('(', ' ')\
        .replace(')', ' ')\
        .replace(',', ' ')\
        .replace(';', ' ')\
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
    print(tracker.as_psql())
logging.info('Processing done.')
