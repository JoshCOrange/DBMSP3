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
def search_table(Dict, tree):  okay
def drop_index(tree):  okay
def drop_table(schemaDict, tree):  okay

    
#test multiple tables --> okay
#test multiple rows  --> okay
#test foreign keys
#when drop a table, should check this table's foreign_start first (arrow start), then go to other tables to delete their foreign_end (arrow end/ this table's dictionary)
'''

def test_create_internal_table():
    create_internal_table()


def test_create_table(schemaDict_tables):#both finish checking insert_internal_table(row)
    create_table(schemaDict_tables)


def test_create_index_tree():
    tree = create_index_tree()
    return tree
    
    
def test_insert_table(schemaDict, tree):
    insert_table(schemaDict, tree)
    #return tree


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
    val = search_table(Dict, tree)
    print(val)


def test_drop_index(tree, table_name):
    drop_index(tree, table_name)
    for i in tree.iteritems():
        #print(i)
        break


def test_drop_table(schemaDict, tree):
    drop_table(schemaDict, tree)
    


if __name__ == '__main__':
    pd.set_option("display.max_colwidth", 10000)   #remind Nitin adding this to execution
    
    schemaDict_tables_1 = {   #for create_table
        "table_name": "ABC",
        "primary_key": ["name_x2"],
        "column_names": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "column_types": ["integer", "string", "integer", "integer"],
        "foreign_keys": [],
        "foreign_tables": [],
        "foreign_columns": [],
        "foreign_deletes": []
    }
    
    schemaDict_tables_2 = {   #for create_table
        "table_name": "DEF",
        "primary_key": ["ID"],
        "column_names": ["Name", "ID", "Apt#"],
        "column_types": ["string", "integer", "integer"],
        "foreign_keys": [],
        "foreign_tables": [],
        "foreign_columns": [],
        "foreign_deletes": []
    }
    
    schemaDict_table1_insert_1 = {
        "table": "ABC",
        "columns": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "values": [123, "dfgjeirg", 456, 789]
    }
    
    schemaDict_table1_insert_2 = {
        "table": "ABC",
        "columns": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "values": [45464, "nnnnnnnn", 8888, 666666]
    }
    
    schemaDict_table1_insert_3 = {
        "table": "ABC",
        "columns": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "values": [222222, "kkklll", 54, 100]
    }
    
    
    schemaDict_table2_insert_1 = {
        "table": "DEF",
        "columns": ["ID", "Name", "Apt#"],
        "values": [168763, "Alice", 101]
    }
    
    schemaDict_table2_insert_2 = {
        "table": "DEF",
        "columns": ["ID", "Name", "Apt#"],
        "values": [54637, "Bob", 560]
    }
    
    schemaDict_table2_insert_3 = {
        "table": "DEF",
        "columns": ["ID", "Name", "Apt#"],
        "values": [121212, "Eve", 489]
    }
    
    schemaDict_table2_insert_4 = {
        "table": "DEF",
        "columns": ["ID", "Name", "Apt#"],
        "values": [666645, "Amy", 335]
    }
    
    
    test_create_internal_table()
    
    test_create_table(schemaDict_tables_1)
    test_create_table(schemaDict_tables_2)
    
    tree_1 = test_create_index_tree()
    tree_2 = test_create_index_tree()
    
    test_insert_table(schemaDict_table1_insert_1, tree_1)
    test_insert_table(schemaDict_table2_insert_1, tree_2)
    
    test_insert_table(schemaDict_table1_insert_2, tree_1)
    test_insert_table(schemaDict_table2_insert_2, tree_2)
    
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
    
    schemaDict_tree1_update_1 = {   #modify primary key
        "table": "ABC",
        "columns": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "values": [123, "sdohboask", 456, 449846987]
    }
    
    schemaDict_tree1_update_2 = {   #modify primary key
        "table": "ABC",
        "columns": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "values": [45464, "snnnnnnn", 8888, 666666]
    }
    
    '''
    schemaDict_tree2_update = {   #not modify primary key
        "table": "DEF",
        "columns": ["ID", "Phone", "Apt#"],
        "values": [168763, 11111111111, 101]
    }
    '''
    
    schemaDict_tree2_update_1 = {   #modify primary key
        "table": "DEF",
        "columns": ["ID", "Name", "Apt#"],
        "values": [5000, "Angel", 101]
    }
    
    schemaDict_tree2_update_2 = {   #modify primary key
        "table": "DEF",
        "columns": ["ID", "Name", "Apt#"],
        "values": [9999999, "Amy", 777]
    }
    
    tree_1 = test_update_table(tree_1, schemaDict_tree1_update_1, "dfgjeirg")
    tree_2 = test_update_table(tree_2, schemaDict_tree2_update_1, 168763)
    
    test_insert_table(schemaDict_table1_insert_3, tree_1)
    test_insert_table(schemaDict_table2_insert_3, tree_2)
    test_insert_table(schemaDict_table2_insert_4, tree_2)
    
    tree_1 = test_update_table(tree_1, schemaDict_tree1_update_2, "nnnnnnnn")
    tree_2 = test_update_table(tree_2, schemaDict_tree2_update_2, 666645)
    
    '''
    for i in tree_1.iteritems():
        print(i)
    for i in tree_2.iteritems():
        print(i)
    '''
    
    #test_delete_index_tree(tree_1, "sdohboask", "ABC")
    #test_delete_index_tree(tree_2, 121212, "DEF")
    
    '''
    for i in tree_1.iteritems():
        print(i)
    for i in tree_2.iteritems():
        print(i)
    '''
    
    Dict_1 = {
        "table_name": "ABC",
        "column_name": ["name_x1", "name_x4"],
        #"column_name": ["*"],
        #"where":{'condition': ['name_x2 LIKE ‘s%’']}
        #"where":{'condition': ['name_x1 BETWEEN 100 AND 50000']}   #non primary key(bottom)
        #"where":{'condition': ['name_x1 BETWEEN 300000 AND 2000000']}
        #"where":{'condition': ['name_x1 BETWEEN 1 AND 52']}
        #"where":{'condition': ['name_x3 IN (54,8888)']}
        #"where":{'condition': ['name_x1 IN (100,112,222222)']}
        #"where":{'condition': ['name_x2 IN ("seflj;k;","sdohboask","snnnnnnn")']}  #string
        #"where":{'condition': ['name_x2 IN ("seflj;k;","snnnnnnn","sdohboask")']}
        #"where":{'condition': ['name_x2 IN ("seflj;k;","ssjdfjsk")']}
        #"where":{'condition': ['name_x1 = 222222']}
        #"where":{'condition': ['name_x3 = 54']}
        #"where":{'condition': ['name_x2 = "kkklll"']}
        #"where":{'condition': ['name_x2 = "jfkpj"']}
        #"where":{'condition': ['name_x1 > 50']}
        #"where":{'condition': ['name_x1 < 1000']}
        #"where":{'condition': ['name_x1 >= 100']}
        #"where":{'condition': ['name_x1 <= 1000']}
        #"where":{'condition': ['name_x1 != 45464']}
        "where":{'condition': ['name_x2 != "zzzzz"']}
    }
    
    Dict_2 = {
        "table_name": "DEF",
        "column_name": ["ID", "Name"],
        #"column_name": ["*"],
        #"where":{'condition': ['Name LIKE ‘E%’']}   #sting, this test case no string
        #"where":{'condition': ['Apt# BETWEEN 100 AND 1000']}   #non primary key(bottom)
        #"where":{'condition': ['Apt# BETWEEN 5 AND 10']}
        #"where":{'condition': ['Apt# BETWEEN 20000000000000 AND 200000000000000']}
        #"where":{'condition': ['ID BETWEEN 1000 AND 6500']}
        #"where":{'condition': ['ID BETWEEN 100 AND 650']}
        #"where":{'condition': ['ID BETWEEN 10000 AND 65000']}
        #"where":{'condition': ['Name IN ("Angel","Kate","Emily","Eve")']}
        #"where":{'condition': ['Name IN ("Kate","Emily")']}
        #"where":{'condition': ['Apt# IN (333,560,5464,101)']}
        #"where":{'condition': ['ID IN (9999999,101,5000,121212)']}
        #"where":{'condition': ['Name = Bob']}
        #"where":{'condition': ['ID = 121212']}
        #"where":{'condition': ['Apt# > 1000']}
        #"where":{'condition': ['ID > 5600']}
        #"where":{'condition': ['Apt# < 500']}
        #"where":{'condition': ['ID < 6500']}
        #"where":{'condition': ['Phone >= 4654531345878']}
        #"where":{'condition': ['ID >= 6825']}
        #"where":{'condition': ['Phone <= 4654531345878']}
        #"where":{'condition': ['ID <= 9622']}
        #"where":{'condition': ['Name != "Kate"']}
        "where":{'condition': ['ID != 100']}
    }
    
    #test_search_table(Dict_1, tree_1)
    #test_search_table(Dict_2, tree_2)
    
    #test_drop_index(tree_1, "ABC")
    #test_drop_index(tree_2, "DEF")
    
    '''
    for i in tree_1.iteritems():
        print(i)
    for i in tree_2.iteritems():
        print(i)
    '''
    
    #test_drop_table(schemaDict_tables_1, tree_1)
    #test_drop_table(schemaDict_tables_2, tree_2)
    
    

