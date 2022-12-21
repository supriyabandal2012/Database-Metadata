import json
import os

from Application.Models.Table import Table, serialize, deserialize
from Application.Variables import PA4_PATH
from Services.Utils import is_lock

# STATIC variables
# used to check for command words
commands = [
    "create",
    "drop",
    "select",
    "alter",
    "use"
]
database_entities = [
    "table",
    "database"
]
data_access_commands = [
    "select",
    "from",
    "where",
    "on"
    # "update",
    # "insert"
]
expects_params_commands = [
    "table",
    "database"
    "select",
    "from",
    "add"
]

metadata_commands = [
    "create",
    "drop",
    "alter",
    "use"
]

data_change_commands = [
    "update",
    "delete",
    "set",
    "where"
]

insert_commands = [
    "insert",
    "into",
    "values"
]

join_commands = {
    "join",
    "inner",
    "outer",
    "left",
    "right"
}

transaction_commands = {
    "transaction",
    "begin",
    "commit"
}

# data access query execution
# isolates clauses (where, from, etc)
def data_access(query, settings):
    if settings["database"] is None:
        print("No database being used.")
        return

    aggregated_records = []

    # processing of non join command query with multiple from parameters.
    if len(query.from_key) > 1 and len(query.join_key) is 0:
        # from
        tables = get_tables(query.from_key, settings)

        # where
        if query.where_key is not None and query.where_key != []:
            a_key = query.where_key[0]
            ineq = query.where_key[1]
            b_key = query.where_key[2]
            tables_keys = list(tables.keys())

            # loop through each record of tbl1 and check against each record of tbl2
            # join type aggregation
            for tbl1_record in tables[tables_keys[0]].records:
                for tbl2_record in tables[tables_keys[1]].records:
                    a = b = None
                    if '.' in a_key:
                        alias = a_key.split('.')[0]
                        key = a_key.split('.')[1]
                        a_key_index = list(tables[alias].metadata)
                        a_key_index = [item.lower() for item in a_key_index].index(key)
                        a = tbl1_record[a_key_index]

                    if '.' in b_key:
                        alias = b_key.split('.')[0]
                        key = b_key.split('.')[1]
                        b_key_index = list(tables[alias].metadata)
                        b_key_index = [item.lower() for item in b_key_index].index(key)
                        b = tbl2_record[b_key_index]

                    if inequalities[ineq](a, b):
                        record = tbl1_record + tbl2_record
                        aggregated_records.append(record)

        # select
        # testing select specific columns
        if query.select_key[0] is not '*':
            tables_keys = tables.keys()
            keys = []

        return Table({**tables[tables_keys[0]].metadata, **tables[tables_keys[1]].metadata}, aggregated_records)

    # processing of non join command query with single from parameter.
    elif len(query.from_key) is 1 and len(query.join_key) is 0:
        # from
        table = get_table(query.from_key, settings)

        # where
        if query.where_key is not None and query.where_key != []:
            key = query.where_key[0]
            ineq = query.where_key[1]
            b = query.where_key[2]
            key_index = list(table.metadata).index(key)
            for record in table.records:
                a = record[key_index]
                if inequalities[ineq](a, b):  # if true
                    aggregated_records.append(record)

        if query.select_key[0] is not '*':
            keys = []
            for column in query.select_key:
                column_index = list(table.metadata).index(column)
                keys.append(column_index)

            final_table = []
            for record in aggregated_records:
                transformed_record = []
                for key in keys:
                    transformed_record.append(record[key])
                final_table.append(transformed_record)

            new_metadata = {}
            for column in query.select_key:
                new_metadata[column] = table.metadata[column]

            return Table(new_metadata, final_table)

        # return original table if no select/where
        return table

    # processing of join command query
    if query.join_key is not None and query.join_key != []:
        tables = get_tables(query.from_key, settings)
        tables_keys = list(tables.keys())

        # detect inner or outer commands and call the proper join function.
        if "inner" in query.join_key:
            aggregated_records = inner_join(query, settings)
        if "outer" in query.join_key:
            aggregated_records = outer_join(query, settings)

        # select
        # testing select specific columns
        if query.select_key[0] is not '*':
            tables_keys = tables.keys()
            keys = []

        return Table({**tables[tables_keys[0]].metadata, **tables[tables_keys[1]].metadata}, aggregated_records)


# function for inner join queries.
# uses nested for loops to process the tables.
def inner_join(query, settings):
    # get tables from the join clause
    # from
    tables = get_tables(query.from_key, settings)

    # on -- condition
    if query.on_key is not None and query.on_key != []:
        a_key = query.on_key[0]
        ineq = query.on_key[1]
        b_key = query.on_key[2]
        tables_keys = list(tables.keys())

        aggregated_records = []

        # loop through each record of tbl1 and check against each record of tbl2
        # join type aggregation
        for tbl1_record in tables[tables_keys[0]].records:
            for tbl2_record in tables[tables_keys[1]].records:
                a = b = None
                if '.' in a_key:
                    alias = a_key.split('.')[0]
                    key = a_key.split('.')[1]
                    a_key_index = list(tables[alias].metadata)
                    a_key_index = [item.lower() for item in a_key_index].index(key)
                    a = tbl1_record[a_key_index]

                if '.' in b_key:
                    alias = b_key.split('.')[0]
                    key = b_key.split('.')[1]
                    b_key_index = list(tables[alias].metadata)
                    b_key_index = [item.lower() for item in b_key_index].index(key)
                    b = tbl2_record[b_key_index]

                if inequalities[ineq](a, b):
                    record = tbl1_record + tbl2_record
                    aggregated_records.append(record)

        return aggregated_records


# function for outer join queries.
# uses nested for loops to process the tables.
def outer_join(query, settings):
    # get tables from the join clause
    # from
    tables = get_tables(query.from_key, settings)

    # on -- condition
    if query.on_key is not None and query.on_key != []:
        a_key = query.on_key[0]
        ineq = query.on_key[1]
        b_key = query.on_key[2]
        tables_keys = list(tables.keys())

        aggregated_records = []

        # loop through each record of tbl1 and check against each record of tbl2
        # join type aggregation
        for tbl1_record in tables[tables_keys[0]].records:
            for tbl2_record in tables[tables_keys[1]].records:
                a = b = None
                if '.' in a_key:
                    alias = a_key.split('.')[0]
                    key = a_key.split('.')[1]
                    a_key_index = list(tables[alias].metadata)
                    a_key_index = [item.lower() for item in a_key_index].index(key)
                    a = tbl1_record[a_key_index]

                if '.' in b_key:
                    alias = b_key.split('.')[0]
                    key = b_key.split('.')[1]
                    b_key_index = list(tables[alias].metadata)
                    b_key_index = [item.lower() for item in b_key_index].index(key)
                    b = tbl2_record[b_key_index]

                # detect left or right join and create the proper initial table.
                # does not include duplicates of no match rows.
                if "left" in query.join_key:
                    if inequalities[ineq](a, b):
                        record = tbl1_record + tbl2_record
                        aggregated_records.append(record)
                    else:
                        record = tbl1_record + [None]*len(tbl2_record)
                        if record not in aggregated_records:
                            aggregated_records.append(record)

                if "right" in query.join_key:
                    if inequalities[ineq](a, b):
                        record = tbl1_record + tbl2_record
                        aggregated_records.append(record)
                    else:
                        record = [None]*len(tbl1_record) + tbl2_record
                        if record not in aggregated_records:
                            aggregated_records.append(record)

        # clean up table to have the proper output.
        # removes a no match row if other matches of the same key exist.
        if "left" in query.join_key:
            if '.' in a_key:
                alias = a_key.split('.')[0]
                key = a_key.split('.')[1]
                a_tbl_len = len(list(tables[alias].metadata))
                a_key_index = list(tables[alias].metadata)
                a_key_index = [item.lower() for item in a_key_index].index(key)

            if '.' in b_key:
                alias = b_key.split('.')[0]
                key = b_key.split('.')[1]
                b_tbl_len = len(list(tables[alias].metadata))
                b_key_index = list(tables[alias].metadata)
                b_key_index = [item.lower() for item in b_key_index].index(key)

            tbl1_start_index = 0
            tbl2_start_index = a_tbl_len

            cleaned_aggregated_record = []
            for aggregated_record in aggregated_records:
                # Look for empty joins (right side)
                if [None]*b_tbl_len == aggregated_record[tbl2_start_index:]:
                    current_record_index = aggregated_records.index(aggregated_record)
                    # case: when it is the first record
                    if aggregated_record == aggregated_records[0]:
                        next_record = aggregated_records[current_record_index + 1]
                        # check if next record tbl2 side is empty. No match from join
                        if next_record[tbl2_start_index:] != [None]*b_tbl_len:
                            # check if next record tbl1 side is the same as the current record tbl1 side
                            if next_record[:tbl2_start_index] == aggregated_record[:tbl2_start_index]:
                                continue
                    # case: when it is the last record
                    elif aggregated_record == aggregated_records[-1]:
                        prev_record = aggregated_records[-2]
                        # check if prev record tbl2 side is empty. No match from join
                        if prev_record[tbl2_start_index:] != [None]*b_tbl_len:
                            # check if prev record tbl1 side is the same as the current record tbl1 side
                            if prev_record[:tbl2_start_index] == aggregated_record[:tbl2_start_index]:
                                continue
                    # case: general
                    else:
                        prev_record = aggregated_records[current_record_index - 1]
                        # check if prev record tbl2 side is empty. No match from join
                        if prev_record[tbl2_start_index:] != [None]*b_tbl_len:
                            # check if prev record tbl1 side is the same as the current record tbl1 side
                            if prev_record[:tbl2_start_index] == aggregated_record[:tbl2_start_index]:
                                continue
                        next_record = aggregated_records[current_record_index + 1]
                        # check if next record tbl2 side is empty. No match from join
                        if next_record[tbl2_start_index:] != [None]*b_tbl_len:
                            # check if next record tbl1 side is the same as the current record tbl1 side
                            if next_record[:tbl2_start_index] == aggregated_record[:tbl2_start_index]:
                                continue
                cleaned_aggregated_record.append(aggregated_record)

        return cleaned_aggregated_record


# check inequality functions below
def equal(a, b):
    return a == b
    pass


def not_equal(a, b):
    return a != b
    pass


def greater_than(a, b):
    return float(a) > float(b)


def less_than(a, b):
    return float(a) < float(b)


# update type query execution
# isolates clauses (where, from, etc)
def update(query, settings):
    if settings["database"] is None:
        print("No database being used.")
        return

    # A locked table will not update
    if is_lock(query.from_key, settings["database"]):
        print(f"Error: Table {query.from_key} is locked!")
        return

    table = get_table([query.from_key], settings)

    # where
    if query.where_key is not None and query.where_key != []:
        inequalities = {
            "=": equal,
            "!=": not_equal,
            ">": greater_than,
            "<": less_than
        }
        counter = 0
        key = query.where_key[0]
        ineq = query.where_key[1]
        b = query.where_key[2]
        key_index = list(table.metadata).index(key)
        set_key = query.set_key[0]
        set_key_index = list(table.metadata).index(set_key)
        for record in table.records:
            a = record[key_index]
            if inequalities[ineq](a, b):
                counter += 1
                record[set_key_index] = query.set_key[2]

        # an active transaction will cache the changes instead of saving permanently
        if settings["transaction"] is True:
            table.title = query.from_key
            settings["cache"][table.title] = table
            table_lock(table, settings)
        else:
            database = settings["database"]
            table_title = query.from_key
            with open(f"PA4/{database}/{table_title}.txt", "w") as table_file:
                table_file.write(json.dumps(serialize(table)))

        print(f"{counter} {'records' if counter > 1 else 'record'} modified.")


# delete type query execution
# isolates clauses (where, from, etc)
def delete(query, settings):
    if settings["database"] is None:
        print("No database being used.")
        return

    table = get_table([query.from_key], settings)

    # where
    if query.where_key is not None and query.where_key != []:
        inequalities = {
            "=": equal,
            "!=": not_equal,
            ">": greater_than,
            "<": less_than
        }
        counter = 0
        new_records = []
        key = query.where_key[0]
        ineq = query.where_key[1]
        b = query.where_key[2]
        key_index = list(table.metadata).index(key)
        for record in table.records:
            a = record[key_index]
            if inequalities[ineq](a, b):
                counter += 1
                continue
            new_records.append(record)

        database = settings["database"]
        table_title = query.from_key
        new_table = Table(table.metadata, new_records)
        with open(f"PA4/{database}/{table_title}.txt", "w") as table_file:
            table_file.write(json.dumps(serialize(new_table)))

        print(f"{counter} {'records' if counter > 1 else 'record'} deleted.")


# insert type query execution
def insert(query, settings):
    if settings["database"] is None:
        print("No database being used.")
        return

    table_title = query.title
    table = get_table([table_title], settings)
    table.records.append(query.values)

    if settings["transaction"] is True:
        settings["cache"][table.title] = table
        print("1 new record inserted.")
        return

    database = settings["database"]
    with open(f"PA4/{database}/{table_title}.txt", "w") as table_file:
        table_file.write(json.dumps(serialize(table)))
    print("1 new record inserted.")


# retrieve table
def get_table(table, settings):
    database = settings["database"]
    check_path = os.path.abspath(f"{PA4_PATH}/{database.strip()}/{table[0]}.txt")
    is_exist = os.path.exists(check_path)
    if is_exist:
        with open(f"./PA4/{database.strip()}/{table[0]}.txt") as _table:
            table_serialized = json.load(_table)
            tbl = deserialize(table_serialized)
            tbl.title = table[0]
        return tbl
    else:
        print(f"{table[0]} table does not exist.")
        return None


# retrieve tables.
# multiple from params
def get_tables(tables, settings):
    database = settings["database"]
    tables_instance = {}
    for table in tables:
        check_path = os.path.abspath(f"{PA4_PATH}/{database.strip()}/{table}.txt")
        is_exist = os.path.exists(check_path)
        if is_exist:
            with open(f"{PA4_PATH}/{database.strip()}/{table}.txt") as _table:
                _table_serialized = json.load(_table)
                tables_instance[table] = deserialize(_table_serialized)
        # else means that the table does not exist. Possibly an alias.
        else:
            try:
                alias = table
                i = tables.index(alias)
                tables_instance[alias] = tables_instance[tables[i - 1]]
                del tables_instance[tables[i - 1]]
            except:
                print(f"Invalid table/s detected.")
                return None
    return tables_instance


# Selects the database to use
# Allows to switch databases if multiple exists
def use_database(params, settings):
    title = params
    check_path = os.path.abspath(f"./PA4/{title}")
    is_exist = os.path.exists(check_path.strip())
    if is_exist:
        settings["database"] = title
        print(f"Using database {title}.")
    else:
        print(f"Database {title} does not exist.")


# Function to create a database
# It creates a folder inside PA4
def create_database(title, settings):
    # check if database exist
    check_path = os.path.abspath(f"./PA4/{title}")
    is_exist = os.path.exists(check_path.strip())
    if is_exist:
        print(f"!Failed to create database {title} because it already exists.")
    else:
        os.system(f'cd PA4 && mkdir {title}')
        settings["database"] = title
        print(f"Database {title} created.")

    # stream = os.popen('cd ')
    # print(stream.read())


# Deletes a database
# goes into PA4 folder and delete the database (folder) if it exist
def drop_database(title, settings):
    check_path = os.path.abspath(f"./PA4/{title}")
    is_exist = os.path.exists(check_path.strip())
    if is_exist:
        os.system(f"rm -rf PA4/{title}")
        print(f"Database {title} deleted.")
        if settings["database"] == title:
            settings["database"] = None
    else:
        print(f"Database {title} does not exist.")


def create_table(title, params, settings):
    if settings["database"] is None:
        print("No database being used.")
        return

    database = settings["database"]
    table_title = title
    check_path = os.path.abspath(f"./PA4/{database}/{table_title}.txt")
    is_exist = os.path.exists(check_path)
    if is_exist:
        print(f"!Failed to create table {table_title} because it already exist.")
        return

    columns = {}
    for param in params:
        data = param.split(' ')
        columns[data[0]] = data[1]

    table = Table(columns, [])

    os.system(f"cd PA4/{database} && touch {table_title}.txt")
    with open(f"PA4/{database}/{table_title}.txt", "w") as table_file:
        table_file.write(json.dumps(serialize(table)))
    print(f"Table {table_title} created.")


# Deletes a table from the selected database
# Goes into the database folder and deletes the table file if it exists
def drop_table(params, settings):
    title = params.strip()
    if settings["database"]:
        check_path = os.path.abspath(f"./PA4/{settings['database'].strip()}/{title}.txt")
        is_exist = os.path.exists(check_path)
        if is_exist:
            os.system(f"rm PA4/{settings['database'].strip()}/{title}.txt")
            print(f"Table {title} deleted.")
        else:
            print(f"Failed to delete table {title} because it does not exist.")
    else:
        print("No database being used.")


# Modifies a table
# Opens the file in the selected database and modifies its metadata values
def alter_table(title, params, settings):
    database = settings["database"]

    if database is None or database is "":
        print("No database being used.")
        return

    check_path = os.path.abspath(f"{PA4_PATH}/{database}/{title}.txt")
    is_exist = os.path.exists(check_path)
    if is_exist:
        with open(f"./PA4/{database.strip()}/{title}.txt") as table:
            data = json.load(table)

        columns = {}
        prop_title = ""
        prop_type = ""
        for prop in params[1].split():
            if prop_title and prop_type:
                if prop_type == "varchar" or prop_type == "char":
                    prop_type += f'({params.split()[params.split().index(prop_type) + 2]})'
                columns[prop_title] = prop_type
                prop_title = ""
                prop_type = ""
            if prop != "(" and prop != ")":
                if not prop_title:
                    prop_title = prop
                else:
                    prop_type = prop

            # params_split = params.split()
            # data[params_split[0]] = params_split[1]

        if len(columns) == 0:
            columns[prop_title] = prop_type
        for k, v in columns.items():
            data[k] = v
        os.system(f'cd PA4/{database.strip()} && touch {title}.txt')
        with open(f'PA4/{database.strip()}/{title}.txt', 'w') as table:
            table.write(json.dumps(data))
        print(f"Table {title} modified.")
    else:
        print(f"table {title} does not exist.")


# [Obsolete] Query a table
# Allows to access the data inside a table from a database
# Works with Select command
def table_query(select_cond, from_table, settings):
    database = settings["database"]

    if database is None or database is "":
        print("No database being used.")
        return

    check_path = os.path.abspath(f"./PA4/{database.strip()}/{from_table}.txt")
    is_exist = os.path.exists(check_path)
    if is_exist:
        with open(f"./PA4/{database.strip()}/{from_table}.txt") as table:
            data = json.load(table)
            ss = ""
            for k, v in data.items():
                ss += f"{k} {v} | "
            print(ss[:-2])
    else:
        print(f"table {from_table} does not exist.")


# Sets the transaction settings to True
# Indicate that a transaction is started/in progress
def begin_transaction(settings):
    settings["transaction"] = True
    print("Transaction starts.")


# Commits a transaction
# Any updates to tables will be saved permanently.
# Updated data is in cache
def commit_transaction(settings):
    cache = settings["cache"]

    for table in cache.items():
        database = settings["database"]
        with open(f"PA4/{database}/{table[1].title}.txt", "w") as table_file:
            table_file.write(json.dumps(serialize(table[1])))
        table_unlock(table[1], settings)

    settings["transaction"] = False  # End transaction
    settings["cache"] = {}  # Empty cache
    if cache:
        print("Transaction committed.")
    else:
        print("Transaction abort.")


# Lock a table by creating a lock file
def table_lock(table, settings):
    database = settings["database"]
    os.system(f"cd PA4/{database} && touch {table.title}.lock")


# Unlock a table by removing the lock file
def table_unlock(table, settings):
    database = settings["database"]
    check_path = os.path.abspath(f"./PA4/{database}/{table.title}.lock")
    is_exist = os.path.exists(check_path)
    if is_exist:
        os.system(f"rm PA4/{database}/{table.title}.lock")




# inequalities function delegate
inequalities = {
    "=": equal,
    "!=": not_equal,
    ">": greater_than,
    "<": less_than
}
