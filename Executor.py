from Application.Commands.Commands import *
from Application.Models.Query import Metadata, DataAccess, Insert, DataChange, Transaction


# Executor class
# Adds execution context to the parsed query.
# This allows for function mapping based on commands and its respective parameters.
class Executor:
    def __int__(self):
        pass

    # A better abstraction for creating a context for each query type
    def create_context(self, query, settings):
        context = {}
        if isinstance(query, Metadata):
            self.command_context[query.query_type](self, query, settings, context)
        if isinstance(query, DataAccess):
            self.command_context["dataAccess"](self, query, settings, context)
        if isinstance(query, DataChange):
            self.command_context["dataChange"](self, query, settings, context)
        if isinstance(query, Insert):
            self.command_context["insert"](self, query, settings, context)
        if isinstance(query, Transaction):
            self.command_context["transaction"](self, query, settings, context)
        return context

    # Data access query type command context
    def data_access_command_context(self, query, settings, context):
        context["run"] = data_access
        context["params"] = [query, settings]

    # Data change query type command context
    def data_change_command_context(self, query, settings, context):
        if query.change_type == "update":
            context["run"] = update
            context["params"] = [query, settings]

        if query.change_type == "delete":
            context["run"] = delete
            context["params"] = [query, settings]

    # Insert query type command context
    def insert_command_context(self, query, settings, context):
        context["run"] = insert
        context["params"] = [query, settings]

    # [Obsolete] Generates the execution context of the parsed queries.
    # Returns an organized function calls blueprint.
    def generate_context(self, tokens, settings):
        context = {}
        self.command_context[tokens[0]](self, tokens, context, settings)
        return context

    # Updated create command context
    def create_command_context(self, query, settings, context):
        metadata = query
        if metadata.entity == "database":
            context["run"] = create_database
            context["params"] = [metadata.title, settings]
        if metadata.entity == "table":
            context["run"] = create_table
            context["params"] = [metadata.title, metadata.params, settings]

    # drop command execution context
    def drop_command_context(self, query, settings, context):
        metadata = query
        if metadata.entity == "database":
            context["run"] = drop_database
            context["params"] = [metadata.title, settings]
        if metadata.entity == "table":
            context["run"] = drop_table
            context["params"] = [metadata.title, settings]

    # use command execution context
    def use_command_context(self, query, settings, context):
        metadata = query
        context["run"] = use_database
        context["params"] = [metadata.title, settings]

    # select command execution context
    def select_command_context(self, query, settings, context):
        context["run"] = data_access
        context["params"] = [query, settings]

    # database command execution context
    def database_command_context(self, tokens, context, settings):
        pass

    # alter command execution context
    def alter_command_context(self, query, settings, context):
        metadata = query
        if metadata.entity == "table":
            context["run"] = alter_table
            context["params"] = [metadata.title, metadata.params, settings]

    # transaction command execution context
    def transaction_command_context(self, query, settings, context):
        if query.begin is True:
            context["run"] = begin_transaction
            context["params"] = [settings]

        if query.commit is True:
            context["run"] = commit_transaction
            context["params"] = [settings]

    # runs the generated execution context
    def run(self, context):
        return context["run"](*context["params"])

    # command context generator function delegate
    command_context = {
        "table": None,
        "select": select_command_context,
        "database": None,
        "from": None,
        "create": create_command_context,
        "drop": drop_command_context,
        "alter": alter_command_context,
        "use": use_command_context,
        "dataAccess": data_access_command_context,
        "dataChange": data_change_command_context,
        "insert": insert_command_context,
        "transaction": transaction_command_context
    }