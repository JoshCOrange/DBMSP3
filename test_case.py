from database import *

'''
def create_internal_table(schemaDict):  okay
def create_table(schemaDict):  okay
def insert_internal_table(row):  okay
def create_index_tree(schemaDict):  okay
def insert_table(schemaDict, tree):  okay
def insert_index_tree(tree, key, row_number):  okay
def search_index_tree(tree, key):  okay
def update_table(tree, schemaDict, key):  okay
def delete_index_tree(tree, key, table_name):  okay
def search_table(Dict, tree):
def drop_table(schemaDict, tree):
def drop_index(tree):
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
    #key = 1
    #row_number = 0
    #tree = insert_index_tree(tree, key, row_number)
    #key = 5
    #row_number = 24
    #tree = insert_index_tree(tree, key, row_number)
    key = "dfgjeirg"
    row_number = 0
    tree = insert_index_tree(tree, key, row_number)
    for i in tree.iteritems():
        #print(i)
        break
    return tree


def test_search_index_tree(tree):
    #key = 5
    #key = 2
    #key = "dfgjeirg"
    key = "fiszjefokp"
    row_number = search_index_tree(tree, key)
    #print(row_number)


def test_update_table(tree):
    '''
    schemaDict = {   #not modify primary key
        "table": "ABC",
        "columns": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "values": [123, "dfgjeirg", 54657345671, 789]
    }
    '''
    schemaDict = {   #modify primary key
        "table": "ABC",
        "columns": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "values": [123, "sdohboask", 456, 449846987]
    }
    
    key = "dfgjeirg"
    tree = update_table(tree, schemaDict, key)
    return tree
    

def test_delete_index_tree(tree):
    for i in tree.iteritems():
        print(i)
    key = "sdohboask"
    table_name = "ABC"
    tree_delete = delete_index_tree(tree, key, table_name)
    #print(tree_delete.has_key(key))


def test_search_table(Dict, tree):
    Dict = {
        "table_name": "ABC",
        "column_name": ["name_x1", "name_x4"],
        "where":
    }
    
    search_table(Dict, tree):


if __name__ == '__main__':
    pd.set_option("display.max_colwidth", 10000)   #remind Nitin adding this to execution
    test_create_internal_table()
    test_create_table()
    test_create_index_tree()
    test_insert_table()
    tree = test_insert_index_tree()
    test_search_index_tree(tree)
    tree_2 = test_update_table(tree)
    tree_3 = test_delete_index_tree(tree_2)
    test_search_table(tree_2)
    
