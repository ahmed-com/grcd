# [TODO]: handle unique constraints
# [TODO]: handle exclusion constraints

from faker import Faker
import psycopg2
import sys
import random
from collections import defaultdict

fake = Faker()

enums = {}

def fetch_enums(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT t.typname, e.enumlabel FROM pg_enum AS e LEFT JOIN pg_type AS t ON e.enumtypid = t.oid WHERE t.typtype = 'e'")
    enum_rows = cursor.fetchall()
    cursor.close()
    for enum_row in enum_rows:
        enum_name, enum_value = enum_row
        if enum_name not in enums:
            enums[enum_name] = []
        enums[enum_name].append(enum_value)


def topological_sort(dependencies):
    """
    Perform a topological sort on the dependency graph.
    """
    visited = set()
    stack = []

    def dfs(node):
        visited.add(node)
        if node in dependencies:
            for neighbor in dependencies[node]:
                if neighbor not in visited:
                    dfs(neighbor)
        stack.append(node)

    for node in dependencies:
        if node not in visited:
            dfs(node)

    return stack[::-1]


def get_all_tables(conn):
    cursor = conn.cursor()

    # Get all table names from the schema
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = cursor.fetchall()
    cursor.close()
    return tables


def get_dependent_tables(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT conname, conrelid::regclass, confrelid::regclass "
        f"FROM pg_constraint "
        f"WHERE confrelid = '{table_name}'::regclass AND contype = 'f'"
    )
    foreign_keys = cursor.fetchall()
    cursor.close()
    return [key[1] for key in foreign_keys]


def order_tables(tables, conn):
    # Build a dependency graph for the tables based on foreign key relationships
    dependencies = {}
    for table in tables:
        table_name = table[0]
        dependent_tables = get_dependent_tables(conn, table_name)
        dependencies[table_name] = dependent_tables

    # Generate a topological order for table seeding based on the dependency graph
    return topological_sort(dependencies)


def get_all_columns(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT c.table_name, c.column_name, c.ordinal_position, CASE WHEN c.data_type = 'USER-DEFINED' THEN t.typname ELSE c.data_type END AS data_type, c.column_default, c.is_nullable, c.character_maximum_length, c.numeric_precision, c.numeric_scale FROM information_schema.columns AS c LEFT JOIN pg_type AS t ON c.udt_name = t.typname WHERE c.table_schema = 'public'")
    columns = cursor.fetchall()
    cursor.close()
    return columns


def generate_random_value(data_type, character_maximum_length=None):
    if data_type == 'integer':
        return fake.random_int()
    elif data_type == 'smallint':
        return fake.random_int(min=-(2 ** 15), max=(2 ** 15 - 1))
    elif data_type == 'bigint':
        return fake.random_int(min=-(2 ** 63), max=(2 ** 63 - 1))
    elif data_type == 'real':
        return fake.random_int() / 100
    elif data_type == 'double precision':
        return fake.random_int() / 100
    elif data_type == 'numeric':
        return fake.random_int() / 100
    elif data_type == 'boolean':
        return fake.boolean()
    elif data_type == 'character varying' or data_type == 'varchar':
        if character_maximum_length is not None:
            return fake.text(max_nb_chars=character_maximum_length)
        else:
            return fake.text()
    elif data_type == 'text':
        return fake.text()
    elif data_type == 'date':
        return fake.date()
    elif data_type == 'time without time zone':
        return fake.time()
    elif data_type == 'timestamp without time zone':
        return fake.date_time()
    elif data_type == 'timestamp with time zone':
        return fake.date_time(tzinfo=fake.pytimezone())
    elif data_type == 'interval':
        return fake.time_delta()
    elif data_type == 'uuid':
        return fake.uuid4()
    elif data_type == 'jsonb':
        return fake.json()
    elif data_type == 'money':
        return fake.currency()
    elif data_type == 'bytea':
        return psycopg2.Binary(fake.binary())
    elif data_type == 'numeric':
        return fake.random_int() / 100
    elif data_type == 'cidr':
        return fake.ipv4()
    elif data_type == 'ARRAY':
        return []
    elif data_type in enums:
        return random.choice(enums[data_type])
    else:
        print(f"Unsupported data type: {data_type}")
        return None

def get_column_value(column, conn):
    cursor = conn.cursor()
    data_type, column_default, is_nullable, character_maximum_length, numeric_precision, numeric_scale = column[3:]

    if column_default is not None and "nextval" in column_default.lower():
        # Handle bigserial column with nextval default value
        sequence_name = column_default.split("'")[1]
        cursor.execute(f"SELECT nextval('{sequence_name}')")
        value = cursor.fetchone()[0]
    # elif column_default is not None:
    #     value = column_default
    elif is_nullable == 'YES':
        value = random.choice([None, generate_random_value(data_type, character_maximum_length)])
    else:
        value = generate_random_value(data_type, character_maximum_length)

    cursor.close()
    return value
        

def get_dependent_columns(conn, column):
    table_name, column_position = column[0], column[2]
    cursor = conn.cursor()
    cursor.execute("SELECT confkey, conrelid::regclass, conkey FROM pg_constraint WHERE contype = 'f' AND confrelid = '{table_name}'::regclass AND {column_position} = ANY(confkey)".format(table_name=table_name, column_position=column_position))
    columns = cursor.fetchall()
    cursor.close()
    dependent_columns = []
    for column in columns:
        index = column[0].index(column_position)
        dependent_columns.append((column[1], column[2][index]))
    return dependent_columns


def seed_table(table_name, table_configuration, conn):
    cursor = conn.cursor()
    sorted_columns = [table_configuration[column_position] for column_position in sorted(table_configuration.keys())]
    try:
        cursor.execute(f"INSERT INTO {table_name} VALUES ({', '.join('%s' for _ in sorted_columns)})", sorted_columns)
    except psycopg2.errors.CheckViolation:
        print(f"Failed to insert into {table_name} with values {sorted_columns}")
        print("please consider removing the check constraint on the table")
        print("to drop all constraints, run the following command:")
        print("""
DO $$
DECLARE
    constraint_name text;
    table_name text;
BEGIN
    -- Loop over check constraints in the specified schema
    FOR constraint_name, table_name IN
        SELECT conname, conrelid::regclass::text
        FROM pg_constraint
        WHERE contype = 'c' -- Filter only check constraints
              AND connamespace = 'public'::regnamespace -- Replace 'your_schema' with the actual schema name
    LOOP
        -- Generate and execute ALTER TABLE statements to drop the check constraints
        EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', table_name, constraint_name);
    END LOOP;
END $$;
        """)
    conn.commit()
    cursor.close()


def seed_all_tables(conn, num_rows):
    columns = get_all_columns(conn)
    tables = get_all_tables(conn)
    ordered_tables = order_tables(tables, conn)
    for _ in range(num_rows):
        configuration = defaultdict(lambda: defaultdict(lambda: None))
        for column in columns:
            table_name, column_position = column[0], column[2]
            if configuration[table_name][column_position] is not None:
                continue
            column_value = get_column_value(column, conn)
            configuration[table_name][column_position] = column_value
            dependent_columns = get_dependent_columns(conn, column)
            for dependent_column in dependent_columns:
                dependent_column_table_name, dependent_column_position = dependent_column
                configuration[dependent_column_table_name][dependent_column_position] = column_value if ordered_tables.index(dependent_column_table_name) >= ordered_tables.index(table_name) else None
        
        for table_name in ordered_tables:
            seed_table(table_name, configuration[table_name], conn)


def truncate_all_tables(conn):
    tables = get_all_tables(conn)
    cursor = conn.cursor()
    for table in tables:
        table_name = table[0]
        cursor.execute(f"TRUNCATE TABLE {table_name} CASCADE")
    conn.commit()
    cursor.close()


def main():
    conn = psycopg2.connect(sys.argv[1])
    truncate_all_tables(conn)
    fetch_enums(conn)
    seed_all_tables(conn, int(sys.argv[2]))

if __name__ == "__main__":
    main()