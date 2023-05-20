import pandas as pd
import connection as conn
from prettytable import PrettyTable
from data.example_data import tournaments_example

FILENAME = "data/atp_tennis.csv"


def get_csv_data(filename):
    data = pd.read_csv(filename)
    return data


def insert_data(collection, data):
    return collection.insert_many(data)


def createTable(headers):
    table = PrettyTable()
    table.field_names = headers
    return table


def add_to_table(table, data):
    for row in data:
        table.add_row(row.values())


if __name__ == '__main__':

    conn.open_connection()

    data = get_csv_data(FILENAME).to_dict(orient='records')

    # IF COLLECTIONS DOES NOT EXIST, MONGO CREATES IT
    tournaments_collection = conn.get_collection('tournaments')

    # INSERTS CSV DATA TO DATABASE
    # !!!! IMPORTANT
    # COMMENT THIS LINE IF YOU ALREADY GOT DATA IN YOUR DATABASE
    insert_data(tournaments_collection, data)
    
    # RETRIEVES DOCUMENTS FROM A COLLECTION
    tournaments_docs = tournaments_collection.find()

    ####### QUERY 1: Inserting many documents #############################
    test = insert_data(tournaments_collection, tournaments_example)
    test_table = createTable(list(tournaments_example[0].keys()))
    add_to_table(test_table, tournaments_example)

    print(test_table)
    print(f"Success?: {test.acknowledged}")
    print(f"Inserted IDs: {test.inserted_ids}")

    ####### QUERY 2: Filtering documents #################################
    query = {"Tournament": "US Open"}
    test2 = tournaments_collection.find(query)
    test_table2 = createTable(list(test2[0].keys()))
    add_to_table(test_table2, test2)
    print(test_table2)

    conn.close_connection()
