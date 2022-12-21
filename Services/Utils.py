# Utils functions. Simple reusable function belong here.
# Also helps to declutter code.
import os


# Removes whitespaces.
# Will eventually target a regular string or a query.
def clean(target, target_type):
    if target_type == "query":
        pass
    if target_type == "string":
        return clean_string(target)
        pass


# Checks if a table is currently locked
def is_lock(table_title, database):
    check_path = os.path.abspath(f"./PA4/{database}/{table_title}.lock")
    is_exist = os.path.exists(check_path)
    return is_exist


# Testing
def clean_string(query):
    return query.strip()

