from database import *

'''
def create_internal_table(schemaDict):  okay
def create_table(schemaDict):  okay
def insert_internal_table(row):  okay
def create_index_tree(schemaDict):  okay
def insert_table(schemaDict, tree):  okay
def insert_index_tree(tree, key, row_number):  okay
def update_table(tree, schemaDict, key):
def delete_index_tree(tree, key):
def drop_table(schemaDict, tree):
def drop_index(tree):
def search_table(Dict, tree):
def search_index_tree(tree, key):
'''

def test_create_internal_table():
    schemaDict_internal_table = {
        "table_name": "Table",
        "primary_key": "table_name",
        "column_name": ["table_name", "schemaDict", "length", "foreign_start", "foreign_end"],
        "column_type": ["string", "dictionary", "integer", "dictionary", "dictionary"]
    }
    
    create_internal_table(schemaDict_internal_table)


def test_create_table():   #both finish checking insert_internal_table(row)
    schemaDict_tables = {
        "table_name": "ABC",
        "primary_key": ["name_x2"],
        "column_name": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "column_type": ["integer", "string", "integer", "integer"],
        "foreign_key": [],
        "foreign_table": [],
        "foreign_column": [],
        "foreign_delete": []
    }
    create_table(schemaDict_tables)


def test_create_index_tree():
    schemaDict_tables = {
        "table_name": "ABC",
        "primary_key": ["name_x2"],
        "column_name": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "column_type": ["integer", "string", "integer", "integer"],
        "foreign_key": [],
        "foreign_table": [],
        "foreign_column": [],
        "foreign_delete": []
    }
    tree = create_index_tree(schemaDict_tables)
    #print(type(tree))
    return tree
    
    
def test_insert_table():
#schemaDict = {'table': tableName, 'columns': columns, 'values': values}
    schemaDict = {
        "table": "ABC",
        "columns": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "values": [123, "dfgjeirg", 456, 789]
    }
    tree = test_create_index_tree()
    insert_table(schemaDict, tree)


def test_insert_index_tree():
    tree = test_create_index_tree()
    key = 1
    row_number = 0
    tree = insert_index_tree(tree, key, row_number)
    for i in tree.iteritems():
        print(i)
    

if __name__ == '__main__':
    pd.set_option("display.max_colwidth", 10000)
    test_create_internal_table()
    test_create_table()
    test_create_index_tree()
    test_insert_table()
    test_insert_index_tree()
