from BTrees.OIBTree import OIBTree
import csv
import pandas as pd

'''
def create_tree:   #index, table, or database
def drop:   #drop a whole table or index
def search:   #select, assume only search one value first

def use:   #define which database to use (no need)
def print_tree:   #print iteritems()

def insert:
def delete:   #no need to do on csv file
def update:   #update_table() last step：according to primary key, use index tree locate row, then update that row (haven't write)
'''

def create_internal_table(schemaDict):
    column_names = schemaDict.get('column_name', [])
    table = pd.DataFrame(columns=column_names)
    table.to_csv("internal_table.csv")
    

def insert_internal_table(schemaDict):
    df = pd.read_csv("internal_table.csv")
    with open("internal_table.csv", 'a', newline='') as f:
        row = df.append(pd.Series(schemaDict['values'], index=df.columns.values))
        df.to_csv("internal_table.csv")


def create_table(schemaDict):   #both for tables and internal table
    #parameter may be a list or dictionary (assume dictionary)
    column_names = schemaDict.get('column_name', [])
    table = pd.DataFrame(columns=column_names)
    #print(table)
    table.to_csv(f"{schemaDict['table_name']}.csv")
    return table   #return a list of file objects


def create_index_tree(schemaDict):
    tree = IIBTree()
    return tree


def insert_table(schemaDict, tree):
    #schemaDict = {'table': tableName, 'columns': columns, 'values': values}
    #check for duplicate on primary key, if existing --> return Error
    df = pd.read_csv("internal_table.csv")
    df_1 = pd.read_csv(f"{schemaDict['table']}.csv")
    with open("internal_table.csv", 'r', newline='') as f:   #find which column is primary_key
        for i in df['table_name']:
            if i == schemaDict['table']:
                specific_row = df.loc[i]
                break
        schema = dict(specific_row[2])   #find its schemaDict
        primary_key_column_name = schema['primary_key']
    
    for i in range(len(schemaDict['columns'])):
        if primary_key_column_name == schemaDict['columns'][i]:
            index = i
            break
    
    if schemaDict['values'][index] in df_1['primary_key']:
        print("Error!!!!!")
    
    with open(f"{schemaDict['table']}.csv", 'a', newline='') as f:
        row = df_1.append(pd.Series(schemaDict['values'], index=df_1.columns.values))
        df_1.to_csv(f"{schemaDict['table']}.csv")
    
    df_2 = pd.read_csv("internal_table.csv")   #for row number
    with open("internal_table.csv", '+', newline='') as f:
        for i in df_2['table_name']:   #in internal table column "table name"
            if i == schemaDict['table']:
                specific_row = df_2.loc[i]
                break
        length = int(specific_row[4])   #find its schemaDict
        length += 1
        specific_row[4] = length
        for i in df_2['table_name']:   #in internal table column "table name"
            if i == schemaDict['table']:
                df_2.loc[i] = specific_row
                break
        df_2.to_csv("internal_table.csv")
    
    insert_index_tree(tree, schemaDict['values'][index], length)   #tree, key, value
    return schemaDict['values'][index], length
    

def insert_index_tree(tree, key, row_number):
    #user can also delete index tree (but we don't want the index tree to be deleted) --> when executing, avoid any index to be called index tree name
    #every node store a key-value pair (primary key, row_number)
    #this is also used in updating index tree
    tree.insert(key, row_number)


def update_table(schemaDict, tree):   #haven't finished
    #schemaDict = {'table': tableName, 'columns': columns, 'values': values}
    delete_index_tree()   #再回來改, 把pointer刪掉
    key, row_number = insert_table(schemaDict, tree)   #append a new row at the end of the csv file
    
    '''
    #用Pandas抓heading出來(先確定column順序)
    #先進internal table找primary key(某一個column name/heading),存下來(該column, value=primary key)
    #進去Schema找到同樣名字的column,把list第幾位抓出來,再去values同一個位置抓出來
    #根據primary key用index tree定位row,然後更新那個row
    
    df_1 = pd.read_csv(f"{schemaDict['table']}.csv")
    df_2 = pd.read_csv("internal_table.csv")
    with open("internal_table.csv", 'r', newline='') as f:
        for i in df_2['table_name']:   #in internal table column "table name"
            if i == schemaDict['table']:
                specific_row = df_2.loc[i]
                break
        schema = dict(specific_row[2])   #find its schemaDict
        primary_key = schema['primary_key']
        
    with open(f"{schemaDict['table']}.csv", 'a', newline='') as f:
        row = df.append(pd.Series(schemaDict['values'], index=df.columns.values))
        #根據primary key用index tree定位row,然後更新那個row
        df.to_csv(f"{schemaDict['table']}.csv")
    '''
    
    
def delete_index_tree(tree, schemaDict=None, key=None):   #待檢查
    #csv file do not involve delete, just index tree
    #consider how to deal with foreign key
    #cascade, set null, restrict
    #parser: delete
    #two mode: given schemaDict or given primary key
    pass


def drop_table(schemaDict):
    df = pd.read_csv(f"{schemaDict['table']}.csv")
    with open(f"{schemaDict['table']}.csv", 'w', newline='') as f:
        df_1 = df.drop(df.index[0:])
        

def drop_index(tree):
    #just delete root
    for key in tree.keys():
        delete_index_tree(tree=tree, key=key)
    tree.clear()
    

def search_table(Dict):
    #Dict = table_name, column_name(要return哪些column), condition(condition_column, Like w%) (a = x)
    #'where': {'conditions': ['name LIKE ‘W%’', 'p# BETWEEN 11 AND 15', 'a_num BETWEEN 20 AND 25', 'p# IN (2,4,8)'] and "=, >, <, >=, <=, !="}
    #condition_column: need to search, column_name: need to return
    #需要回傳所有符合條件的(rows)對應的column
    txt = Dict['where']['conditions'][0]   #condition value
    search_conditions = txt.split()
    search_column_name = search_conditions[0]   #get column name we need to search (only one column)
    
    if search_conditions[1] == "LIKE" or "IN" or ">" or "<" or "=" or ">=" or "<=" or "!=":
        search_value = search_conditions[2]
    elif search_conditions[1] == "BETWEEN":
        search_value_1 = search_conditions[2]
        search_value_2 = search_conditions[4]
    
    df = pd.read_csv(f"{Dict['table_name']}.csv")
    with open(f"{Dict['table_name']}.csv", 'r', newline='') as f:
        #根據condition_column,找到有那個值(ex:w%)的那個row,取出那整個row,再根據column_name看要回傳哪些對應的column的值傳回去
        for i in range(len(Dict['ta'][])):
            
        


def search_index_tree(tree, key):
    if tree.has_key(key) is true:
        row_number = tree.get(key[, default=None])
    else:
        row_number = "Not Found!!!!!"
    return row_number
    

def use():   #define which database to use
    pass
    
    
def print_tree():   #print iteritems()
    pass



if __name__ == '__main__':   #need to be deleted (main function is not here)
    schemaDict = {
        "table_name": "ABC",
        "primary_key": ["name_x1", "name_x2"],
        "column_name": ["name_1", "name_2", "name_3"],
        "column_type": ["integer", "string", "integer"],
        "foreign_key": ["name_x3", "name_x4"],
        "foreign_table": ["T_1", "T_2"],
        "foreign_column": ["C_1", "C_2", "C_3", "C_4"],
        "foreign_delete": ["S_1", "S_2", "S_3"]
    }
    
    schemaDict2 = {
        "table_name": "Table",
        "primary_key": "table_name",
        "column_name": ["table_name", "schemaDict", "Root_Index", "length"],
        "column_type": ["string", "dictionary", "string", "integer"]
    }
    
    create_internal_table(schemaDict2)
    create_table(schemaDict)
    #insert_table(schemaDict)
    #update_table(schemaDict2)
    #drop_table(schemaDict)

'''
f = open(, "w")
then build index tree
每塞一行進csv file加一個node (table & tree build simutaneously)

用什麼格式塞進csv (先試著塞list在一行，讀出來也是list)
需要另存每個column的original type (create table時會用到)
可放在Bplustree最前面

從csv拉出來全為string
'''
