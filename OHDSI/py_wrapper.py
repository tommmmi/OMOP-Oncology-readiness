import psycopg2
import csv
import os
import re

# ----------------------------------------------------------------
# Configuration start

# This is your current working directory:
print(os.getcwd())

# Set the working directory to the path where the script files are located:
# os.chdir("Set_your_working_directory")

# Configure your schema name, replace cdm with your current schema.
# If you don't need a schema, enter an empty string ""
schema = "cdm"

# Choose the sql input file
sqlFile = "episode.sql"

# Set your connection string
connection_string = "dbname = 'Synpuf 5' port=5432 user='postgres' password='postgres'"

# Configuration end
# ----------------------------------------------------------------

try:
    conn = psycopg2.connect(connection_string)
except:
    print("Unable to connect!")

if sqlFile[-4:] != ".sql":
    raise NameError("Sql file must end with .sql")

with open(sqlFile, 'r') as file:
    data = file.read()

if schema != "":
    if schema[-1:] != ".":
        schema = schema + "."

    data = re.sub(r'\bdrug_exposure\b', schema + "drug_exposure", data)
    data = re.sub(r'\bdevice_exposure\b', schema + "device_exposure", data)
    data = re.sub(r'\bprocedure_occurrence\b', schema + "procedure_occurrence", data)
    data = re.sub(r'\bcondition_occurrence\b', schema + "condition_occurrence", data)
    data = re.sub(r'\bobservation\b', schema + "observation", data)
    data = re.sub(r'\bmeasurement\b', schema + "measurement", data)
    data = re.sub(r'\bepisode\b', schema + "episode", data)

a = data.split(";")

with conn.cursor() as curs:
    try:
        for sql in a:
            sql = sql.lstrip()
            if sql[0:6] == "select":
                curs.execute(sql)
                rows = curs.fetchall()
                column_names = [i[0] for i in curs.description]
                f = sqlFile[0:-4] + ".csv"
                fp = open(f, 'w')
                myFile = csv.writer(fp, lineterminator = '\n')
                myFile.writerow(column_names)
                myFile.writerows(rows)
                fp.close()
                print(f + " has been written.")
            elif sql != "":
                curs.execute(sql)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

conn.close()
