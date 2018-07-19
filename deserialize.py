import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

if __name__ == '__main__':
    # Load the schema
    schema = avro.schema.Parse(open("schema.json", "rb").read())

    # Deserialization
    reader = DataFileReader(open("users.avro", "rb"), DatumReader())
    for grade in reader:
        print(repr(grade))
