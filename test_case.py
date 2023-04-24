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
def drop_index(tree):
def drop_table(schemaDict, tree):
'''

def test_create_internal_table():
    schemaDict_internal_table = {
        "table_name": "Table",
        "primary_key": "table_name",
        "column_name": ["table_name", "schemaDict", "length", "foreign_start", "foreign_end"],
        "column_type": ["string", "dictionary", "integer", "dictionary", "dictionary"]
    }
    
    create_internal_table(schemaDict_internal_table)


def test_create_table(schemaDict_tables):#both finish checking insert_internal_table(row)
    create_table(schemaDict_tables)


def test_create_index_tree():
    tree = create_index_tree()
    return tree
    
    
def test_insert_table(schemaDict, tree):
    tree = insert_table(schemaDict, tree)
    return tree


def test_insert_index_tree(tree):
    for i in tree.iteritems():
        #print(i)
        break
    return tree


def test_search_index_tree(tree, key):
    row_number = search_index_tree(tree, key)
    #print(row_number)


def test_update_table(tree, schemaDict, key):
    tree = update_table(tree, schemaDict, key)
    for i in tree.iteritems():
        #print(i)
        break
    return tree
    

def test_delete_index_tree(tree, key, table_name):
    for i in tree.iteritems():
        #print(i)
        break
    tree_delete = delete_index_tree(tree, key, table_name)
    #print(tree_delete.has_key(key))


def test_search_table(Dict, tree):
    #print("----------")
    val = search_table(Dict, tree)
    print(val)


if __name__ == '__main__':
    pd.set_option("display.max_colwidth", 10000)   #remind Nitin adding this to execution
    
    schemaDict_tables_1 = {   #for create_table
        "table_name": "ABC",
        "primary_key": ["name_x2"],
        "column_name": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "column_type": ["integer", "string", "integer", "integer"],
        "foreign_key": [],
        "foreign_table": [],
        "foreign_column": [],
        "foreign_delete": []
    }
    
    schemaDict_tables_2 = {   #for create_table
        "table_name": "DEF",
        "primary_key": ["ID"],
        "column_name": ["Phone", "ID", "Apt#"],
        "column_type": ["integer", "integer", "integer"],
        "foreign_key": [],
        "foreign_table": [],
        "foreign_column": [],
        "foreign_delete": []
    }
    
    schemaDict_table1_insert = {
        "table": "ABC",
        "columns": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "values": [123, "dfgjeirg", 456, 789]
    }
    
    schemaDict_table2_insert = {
        "table": "DEF",
        "columns": ["ID", "Phone", "Apt#"],
        "values": [168763, 4654531345878, 101]
    }
    
    
    test_create_internal_table()
    
    test_create_table(schemaDict_tables_1)
    test_create_table(schemaDict_tables_2)
    
    tree_1 = test_create_index_tree()
    tree_2 = test_create_index_tree()
    
    tree_1 = test_insert_table(schemaDict_table1_insert, tree_1)
    tree_2 = test_insert_table(schemaDict_table2_insert, tree_2)
    
    tree_1 = test_insert_index_tree(tree_1)
    tree_2 = test_insert_index_tree(tree_2)
    
    test_search_index_tree(tree_1, "dfgjeirg")
    test_search_index_tree(tree_2, 564654)
    
    '''
    schemaDict_tree1_update = {   #not modify primary key
        "table": "ABC",
        "columns": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "values": [123, "dfgjeirg", 54657345671, 789]
    }
    '''
    
    schemaDict_tree1_update = {   #modify primary key
        "table": "ABC",
        "columns": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "values": [123, "sdohboask", 456, 449846987]
    }
    
    '''
    schemaDict_tree2_update = {   #not modify primary key
        "table": "DEF",
        "columns": ["ID", "Phone", "Apt#"],
        "values": [168763, 11111111111, 101]
    }
    '''
    
    schemaDict_tree2_update = {   #modify primary key
        "table": "DEF",
        "columns": ["ID", "Phone", "Apt#"],
        "values": [5000, 4654531345878, 101]
    }
    
    tree_1 = test_update_table(tree_1, schemaDict_tree1_update, "dfgjeirg")
    tree_2 = test_update_table(tree_2, schemaDict_tree2_update, 168763)
    
    #test_delete_index_tree(tree_1, "sdohboask", "ABC")
    #test_delete_index_tree(tree_2, 5000, "DEF")
    
    Dict_1 = {
        "table_name": "ABC",
        "column_name": ["name_x1", "name_x4"],
        #"column_name": ["*"],
        #"where":{'condition': ['name_x2 LIKE ‘s%’']}
        #"where":{'condition': ['name_x1 BETWEEN 100 AND 200']}   #non primary key(bottom)
        #"where":{'condition': ['name_x1 BETWEEN 156 AND 200']}
        #"where":{'condition': ['name_x1 BETWEEN 1 AND 52']}
        #"where":{'condition': ['name_x3 IN (123,456,789)']}
        #"where":{'condition': ['name_x1 IN (100,112)']}
        "where":{'condition': ['name_x2 IN ("seflj;k;","sdohboask")']}  #string
    }
    
    Dict_2 = {
        #"table_name": "ABC",
        #"column_name": ["name_x1", "name_x4"],
        #"column_name": ["*"],
        #"where":{'condition': ['name_x2 LIKE ‘s%’']}
        #"where":{'condition': ['name_x1 BETWEEN 100 AND 200']}   #primary key(top)
        #"where":{'condition': ['name_x1 BETWEEN 156 AND 200']}
        #"where":{'condition': ['name_x1 BETWEEN 1 AND 52']}
    }
    
    test_search_table(Dict_1, tree_1)
    #test_search_table(tree_2)
    
    
    
    #test multiple tables
    #test multiple rows
    #test foreign keys
