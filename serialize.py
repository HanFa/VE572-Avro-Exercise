import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

if __name__ == '__main__':
    # Load the schema
    schema = avro.schema.Parse(open("schema.json", "rb").read())

    # Serialization
    writer = DataFileWriter(open("users.avro", "wb"), DatumWriter(), schema)
    with open("input/maxscores.csv") as f:
        lines = f.readlines()
        for line in lines:
            line = line.strip()
            student = line.split('\t')[0]
            maxscore = line.split('\t')[1]
            writer.append({'student' : student, 'maxscore' : int(maxscore)})

    writer.close()
