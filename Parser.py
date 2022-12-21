from Application.Commands.Commands import commands, expects_params_commands, data_access_commands, database_entities, \
    metadata_commands, data_change_commands, insert_commands, transaction_commands
from Application.Models.Query import query_builder
from Services.Utils import clean


# Parser class
# Used to parse the input and can organize and validate the queries.
class Parser:
    def __int__(self):
        pass

    # temp query holder. Allows for building queries.
    query = ""

    # deletes the temp query
    def clean(self):
        self.query = ""

    # Breaks down input by tokens
    def tokenize(self, query):
        trimmed = []
        buffer = ""
        for char in query:
            if char is '(' or char is ')' or char is ';' or char is "'":
                if buffer:
                    trimmed.append(buffer)
                    buffer = ""
                trimmed.append(char)
                continue
            if char is ' ' or char is ',':
                if buffer:
                    trimmed.append(buffer)
                    buffer = ""
                continue
            buffer += char
        return trimmed

    # parse abstraction to create query types
    def parse(self, query):
        tokens = self.tokenize(query)
        if tokens[0] in metadata_commands:
            return query_builder(tokens, "metadata")
        if tokens[0] in data_access_commands:
            return query_builder(tokens, "dataAccess")
        if tokens[0] in data_change_commands:
            return query_builder(tokens, "dataChange")
        if tokens[0] in insert_commands:
            return query_builder(tokens, "insert")
        if tokens[0] in transaction_commands:
            return query_builder(tokens, "transaction")

    # validates incoming inputs
    def validate(self, inp):
        if inp == '':
            return False

        clean_input = clean(inp, "string")
        clean_input_validated = ""
        parenthesis = 0
        quotes = 0
        dash = 0
        for char in clean_input:
            if dash == 2:
                clean_input_validated = clean_input_validated[:-3]
                break
            if char is '-':
                dash += 1
            if char is '(':
                parenthesis += 1
            if char is ')':
                parenthesis -= 1
            if char is "'":
                if quotes is 0:
                    quotes += 1
                    continue
                if quotes is not 0:
                    quotes -= 1
                    continue
            if parenthesis is 0 and quotes is 0:
                clean_input_validated += char.lower()
            if parenthesis > 0 or quotes > 0:
                clean_input_validated += char

        if self.query:
            self.query += " " + clean_input_validated
        else:
            self.query += clean_input_validated

        if clean_input_validated[-1] is not ';':
            return False
        else:
            return True

