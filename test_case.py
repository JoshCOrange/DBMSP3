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
    print(row_number)


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
    #print(val)


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
        "values": [222222, "kkklll", 89566, 100]
    }
    
    
    schemaDict_table2_insert_1 = {
        "table": "DEF",
        "columns": ["ID", "Phone", "Apt#"],
        "values": [168763, 4654531345878, 101]
    }
    
    schemaDict_table2_insert_2 = {
        "table": "DEF",
        "columns": ["ID", "Phone", "Apt#"],
        "values": [54637, 11111111, 560]
    }
    
    schemaDict_table2_insert_3 = {
        "table": "DEF",
        "columns": ["ID", "Phone", "Apt#"],
        "values": [121212, 4000000000, 489]
    }
    
    schemaDict_table2_insert_4 = {
        "table": "DEF",
        "columns": ["ID", "Phone", "Apt#"],
        "values": [666645, 7777446467, 335]
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
    
    test_insert_table(schemaDict_table1_insert_3, tree_1)
    test_insert_table(schemaDict_table2_insert_3, tree_2)
    test_insert_table(schemaDict_table2_insert_4, tree_2)
    

    
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
        #"where":{'condition': ['name_x2 IN ("seflj;k;","sdohboask","sdohboask","sdohboask")']}  #string
        #"where":{'condition': ['name_x2 IN ("seflj;k;","ssjdfjsk")']}
        #"where":{'condition': ['name_x1 = 123']}
        #"where":{'condition': ['name_x2 = "sdohboask"']}
        #"where":{'condition': ['name_x2 = "jfkpj"']}
        #"where":{'condition': ['name_x1 > 50']}
        #"where":{'condition': ['name_x1 < 1000']}
        #"where":{'condition': ['name_x1 >= 100']}
        #"where":{'condition': ['name_x1 <= 1000']}
        #"where":{'condition': ['name_x1 != 1000']}
        "where":{'condition': ['name_x1 != 123']}
    }
    
    Dict_2 = {
        "table_name": "DEF",
        "column_name": ["Apt#", "ID"],
        #Phone: 4654531345878 (1000000000000)
        #"column_name": ["*"],
        #"where":{'condition': ['name_x2 LIKE ‘s%’']}   #sting, this test case no string
        #"where":{'condition': ['Phone BETWEEN 1000000000000 AND 20000000000000']}   #non primary key(bottom)
        #"where":{'condition': ['Phone BETWEEN 100 AND 200']}
        #"where":{'condition': ['Phone BETWEEN 20000000000000 AND 200000000000000']}
        #"where":{'condition': ['ID BETWEEN 1000 AND 6500']}
        #"where":{'condition': ['ID BETWEEN 100 AND 650']}
        #"where":{'condition': ['ID BETWEEN 10000 AND 65000']}
        #"where":{'condition': ['Apt# IN (50,101,563)']}
        #"where":{'condition': ['Apt# IN (50,563)']}
        #"where":{'condition': ['ID IN (43,90,5000,8923)']}
        #"where":{'condition': ['Phone = 30']}
        #"where":{'condition': ['ID = 650']}
        #"where":{'condition': ['Apt# > 1000']}
        #"where":{'condition': ['ID > 5600']}
        #"where":{'condition': ['Apt# < 500']}
        #"where":{'condition': ['ID < 6500']}
        #"where":{'condition': ['Phone >= 4654531345878']}
        #"where":{'condition': ['ID >= 6825']}
        #"where":{'condition': ['Phone <= 4654531345878']}
        #"where":{'condition': ['ID <= 9622']}
        #"where":{'condition': ['Phone != 4654531345878']}
        "where":{'condition': ['ID != 5000']}
    }
    
    #test_search_table(Dict_1, tree_1)
    #test_search_table(Dict_2, tree_2)
    
    #test_drop_index(tree_1, "ABC")
    #test_drop_index(tree_2, "DEF")
    
    #test_drop_table(schemaDict_tables_1, tree_1)
    #test_drop_table(schemaDict_tables_2, tree_2)
    
    
    
    #test multiple tables --> okay
    #test multiple rows
    #test foreign keys
