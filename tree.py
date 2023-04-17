from BTrees.IIBTree import IIBTree
import csv
import pandas as pd

'''
def create_tree:   #index, table, or database  --> how to initialize object
def drop:   #drop一整個table或index  --> clear() + pop()
def search:   #select, 先假設只searh一個值  --> get()

def use:   #define which table to use
def print_tree:   #print iteritems()

def insert:   #insert()
def delete:   #delete certain rows --> pop()  --> no need to do on csv file
def update:  #update() --> update_table最後一步：根據primary key用index tree定位row,然後更新那個row,還未寫
'''

def create_internal_table(schemaDict):
    column_names = schemaDict.get('column_name', [])
    table = pd.DataFrame(columns=column_names)
    table.to_csv("internal_table.csv")
    

def insert_internal_table(schemaDict):
    df = pd.read_csv("internal_table.csv")
    with open("internal_table.csv", 'a', newline='') as f:
        row = df.append(pd.Series(schemaDict['values'], index=df.columns.values))
    #length + 1


def create_table(schemaDict):   #both for tables and internal table
    #parameter may be a list or dictionary (assume dictionary)
    column_names = schemaDict.get('column_name', [])
    table = pd.DataFrame(columns=column_names)
    #print(table)
    table.to_csv(f"{schemaDict['table_name']}.csv")
    return table   #return the list of file objects


def create_index_tree(schemaDict):   #待確認parameter name
    tree = IIBTree()
    return tree


def insert_table(schemaDict):
    #schemaDict = {'table': tableName, 'columns': columns, 'values': values}
    df = pd.read_csv(f"{schemaDict['table']}.csv")
    with open(f"{schemaDict['table']}.csv", 'a', newline='') as f:
        row = df.append(pd.Series(schemaDict['values'], index=df.columns.values))


def insert_index_tree(schemaDict, row_num):
    #user can also delete index tree (but we don't want the index tree to be deleted) --> when executing，avoid any index to be called index tree name
    #primary keys need to be hash(), and avoid collision
    pass


def update_table(schemaDict):
    #schemaDict = {'table': tableName, 'columns': columns, 'values': values}
    '''
    #用Pandas抓heading出來(先確定column順序)
    #先進internal table找primary key(某一個column name/heading), 存下來(該column, value=primary key)
    #進去Schema找到同樣名字的column,把list第幾位抓出來,再去values同一個位置抓出來
    #根據primary key用index tree定位row,然後更新那個row
    '''
    df_1 = pd.read_csv(f"{schemaDict['table']}.csv")
    df_2 = pd.read_csv("internal_table.csv")
    with open("internal_table.csv", 'r', newline='') as f:
        for i in df_2['table_name']:   #in internal table column "table name"
            if i == schemaDict['table']:
                primary_key = i
    
    with open(f"{schemaDict['table']}.csv", 'a', newline='') as f:
        row = df.append(pd.Series(schemaDict['values'], index=df.columns.values))
    #根據primary key用index tree定位row,然後更新那個row


def update_index_tree():
    pass

    
def delete_index_tree():   #csv file do not involve delete, just index tree
    pass


def drop_table(schemaDict):
    df = pd.read_csv(f"{schemaDict['table']}.csv")
    with open(f"{schemaDict['table']}.csv", 'w', newline='') as f:
        df_1 = df.drop(df.index[0:])
        

def drop_index(index_name, table_name):   #index name對應的table_name, both are string
    pass
    #把root delete


def search_table(schemaDict):   #table_name, column_name(要return哪些column), condition(condition_column, Like w%) (a = x)
    #根據condition_column,找到有那個值(Like w%)的那個row,取出那整個row,再根據column_name看要回傳哪些對應的column的值傳回去
    #需要回傳所有符合條件的(rows)對應的column
    pass


def search_index_tree():
    pass


def use():   #define which database to use
    pass
    
    
def print_tree():   #print iteritems()
    pass



if __name__ == '__main__':   #need to be deleted (main is not here)
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
        "column_name": ["table_name", "Dict_CT", "Index"],
        "column_type": ["string", "dictionary", "string"],
        "length":
    }
    
    create_internal_table(schemaDict2)
    create_table(schemaDict)
    #insert_table(schemaDict)
    #update_table(schemaDict2)
    drop_table(schemaDict)

'''
f = open(, "w")
then build index tree
每塞一行進csv file加一個node (table & tree build simutaneously)

用什麼格式塞進csv (先試著塞list在一行，讀出來也是list)
需要另存每個column的original type (create table時會用到)
可放在Bplustree最前面

從csv拉出來全為string
'''
