# Table class. Allows for storing metadata and records.
# Table also has a serialization/deserialization for storing data
# in a consistent way. Uses Json format.

class Table:
    def __init__(self, metadata, records):
        self.metadata = metadata
        self.records = records

    title = None

    # Creates a string representation of the data in the Table.
    def to_string(self):
        header = ""
        for column in self.metadata:
            header += f"{column} {self.metadata[column]} | "
        header = header[:-2]
        header += '\n'

        records = ""
        for record in self.records:
            for data in record:
                if data is None:
                    records += f" | "
                else:
                    records += f"{data} | "
            records = records[:-2]
            records += '\n'

        return header + records


# Serialize table instance into a Json format.
def serialize(table):
    serialized = {
        "metadata": table.metadata,
        "records": table.records
    }
    return serialized


# returns a Table object from a Json format.
def deserialize(json):
    return Table(json["metadata"], json["records"])