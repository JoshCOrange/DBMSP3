from BTrees.OIBTree import OIBTree
import csv
import pandas as pd
import json

'''
def create_tree:   #index, table, or database
def drop:   #drop a whole table or index
def search:   #select, assume only search one value first

def use:   #define which database to use (no need)
def print_tree:   #print iteritems()

def insert:
def delete:   #no need to do on csv file
def update:   #update_table() haven't write
'''

def LIKE(str):
    regex_pattern = "^" + re.sub(
        "[%_]|\[[^]]*\]|[^%_[]+",
        lambda match:
        (".*" if match.group() == "%"
        else "." if match.group() == "_"
        else match.group() if match.group().startswith("[") and match.group().endswith("]")
        else re.escape(match.group())), str
        ) + "$"
    #print(regex_pattern)
    return regex_pattern
    
    
def Like_check(df, column_name, condition): #str = LIKE 的condition
    reg = LIKE(condition)
    mask = df[[column_name]].apply(
        lambda x: x.str.contains(
            reg,
            regex=True
        )
    ).any(axis=1)
    return df[mask]   #return the all rows that fit the condition


def create_internal_table(schemaDict):
    column_names = schemaDict.get('column_name', [])
    table = pd.DataFrame(columns=column_names)
    table.to_csv("internal_table.csv")
    

def insert_internal_table(row):   #parameter shpould be a Dataframe
    df = pd.read_csv("internal_table.csv")
    with open("internal_table.csv", 'a', newline='') as f:
        new_row = df.append(row)
        df.to_csv("internal_table.csv")


def create_table(schemaDict):   #both for tables and internal table
    #parameter may be a list or dictionary (assume dictionary)
    column_names = schemaDict.get('column_name', [])
    table = pd.DataFrame(columns=column_names)
    #print(table)
    '''
        "table_name": "ABC",
        "primary_key": ["name_x1", "name_x2"],
        "column_name": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "column_type": ["integer", "string", "integer", "integer"],
        "foreign_key": ["name_x3", "name_x4"],
        "foreign_table": ["T_1", "T_2"],
        "foreign_column": ["C_1", "C_2"],
        "foreign_delete": ["S_1", "S_2"]
        
        "column_name": ["table_name", "schemaDict", "length", "foreign_start", "foreign_end"]
    '''
    table.to_csv(f"{schemaDict['table_name']}.csv")
    df = pd.read_csv("internal_table.csv")
    with open("internal_table.csv", 'a', newline='') as f:
        this_table_name = schemaDict['table_name']
        length = 0
        foreign_start = {}   #from this table point to other table
        foreign_end = {}   #the target table (have foreign key)  --> it's empty at the start
        '''
        foreign_start = {"table_name_1": {"foreign_column_name": val_1, "point_to_table": val_2,
                        "primary_column_name": val_3},
                        "table_name_2": {"foreign_column_name": val_4, "point_to_table": val_5,
                        "primary_column_name": val_6}}
                        
        foreign_end = {"table_name_1": {"my_primary_column_name": val_1, "from_table": val_2,
                       "foreign_column_name": val_3},
                       "table_name_2": {"my_primary_column_name": val_4, "from_table": val_5,
                       "foreign_column_name": val_6},}
        '''
        for i in range(len(schemaDict['foreign_key'])):   #fill foreign_start
            tmp = {}
            tmp['foreign_column_name'] = schemaDict['foreign_key'][i]
            tmp['point_to_table'] = schemaDict['foreign_table'][i]
            tmp['primary_column_name'] = schemaDict['foreign_column'][i]
            foreign_start[schemaDict['foreign_table'][i]] = tmp
            
            #modify foreign_end of foreign tables
            foreign_table_name = schemaDict['foreign_table'][i]
            modify_row = df.loc[df['table_name'] == foreign_table_name]
            foreign_end = dict(modify_row['foreign_end'])   #may fail
            foreign_tmp = {}
            foreign_tmp['my_primary_column_name'] = schemaDict['foreign_column'][i]
            foreign_tmp['from_table'] = schemaDict['table_name']
            foreign_tmp['foreign_column_name'] = schemaDict['foreign_key'][i]
            foreign_end[schemaDict['table_name']] = foreign_tmp
            
        #write to a new row, then insert into internal table
        new_list = [this_table_name, schemaDict, length, foreign_start, foreign_end]   #if Dictionaries cannot store in Dictionary form, add .items() at the end
        row = pd.DataFrame()
        row = row.append(pd.Series(new_list, index=df.columns.values))
        insert_internal_table(row)
    return table   #return a list of file objects


def create_index_tree(schemaDict):   #haven't finish (return index tree's root locaiton)
    tree = OIBTree()
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
        schema = dict(specific_row[2])   #find its schemaDict/ may fail
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
        length = int(specific_row[3])   #find its length
        length += 1
        specific_row[3] = length
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
    #this is also used in updating index tree (automatically)
    tree.insert(key, row_number)


def update_table(schemaDict, tree):   #haven't finished
    #schemaDict = {'table': tableName, 'columns': columns, 'values': values}
    delete_index_tree(tree, schemaDict=schemaDict)
    key, row_number = insert_table(schemaDict, tree)   #append a new row at the end of the csv fileß
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
    
    
def delete_index_tree(tree, schemaDict=None, key=None):
    #csv file do not involve delete, just delete the node in index tree
    #consider 1. how to deal with foreign key   2. cascade, set null, restrict
    #two modes: given (tree & schemaDict) or given (tree & primary key)
    '''
    1. tree & schemaDict: schemaDict is similar to search_table() (have 'where' clause --> 'where': {'conditions': ['name LIKE ‘W%’', 'p# BETWEEN 11 AND 15', 'a_num BETWEEN 20 AND 25', 'p# IN (2,4,8)'] and "=, >, <, >=, <=, !="})  --> check/reference SQL delete
    2. tree & primary key: we are dropping table
    '''
    #if schemaDict!=None，一開始，依where clause中condition_column去call search_table()，傳回來所有符合條件的rows，再來check foreign_end
        #check foreign_end: if empty->delete directly; else not empty: 去其他表刪(把那一個cell set null),再回來就可以直接刪index tree node--> pop()
    #if key!=None, key = primary, 一開始check foreign_end: if empty->delete directly; else not empty: 去其他表刪(把那一個cell set null),再回來就可以直接刪index tree node--> pop()
    if schemaDict != None:   #schemaDict = {'table': tableName, 'where': whereClause}
        values = search_table(schemaDict, tree)   #all eligible values/ 等等回來處理schemaDict與Dict不同的部分(要return的columns)
        txt = Dict['where']['conditions'][0]   #condition value
        search_conditions = txt.split(' ')
        search_column_name = search_conditions[0]   #get column name we need to search(only one column)
        
        #checking foreign key
        df = pd.read_csv("internal_table.csv")
        with open("internal_table.csv", 'r', newline='') as f:
            for i in df['table_name']:   #in internal table column "table name"
                if i == schemaDict['table']:
                    specific_row = df.loc[i]
                    break
        foreign_end = specific_row[5]   #Dict/find foreign_end (primary key is used by other tables)
        
        #find primary keys first, then delete key-value pair
        df_1 = pd.read_csv(f"{schemaDict['table_name']}.csv")
        with open(f"{schemaDict['table_name']}.csv", 'r', newline='') as f:
            #result = df[df['ID'] > 100]['name']
            #result_array = result.values
            #primary_key是指符合條件的all rows的primary key
            primary_keys = df_1[df_1[search_column_name] == values]['primary_key']   #Pandas Series object
            primary_keys_array = primary_keys.values
            
            if len(foreign_end) == 0:   #no other tables are using my primary key, delete directly
                #現在要找的是符合where clause的所有的rows的primary key,直接pop(primary_key, row_number),以後就不會指向這行了
                for i in primary_keys_array:
                    val = tree.pop(i)   #remove key and return the corresponding value
                    #print(val)   #should be corresponding row number
                    #print(tree.get(primary_key))   #should be None
            else:   #other tables are using my primary key
                #if not empty: 去其他表刪(把那一個cell set null),再回來就可以直接刪index tree node--> pop()
                '''
                foreign_end = {"table_name_1": {"my_primary_column_name": val_1, "from_table": val_2,
                                                "foreign_column_name": val_3},
                               "table_name_2": {"my_primary_column_name": val_4, "from_table": val_5,
                                                "foreign_column_name": val_6},
                }
                '''
                for _, v in foreign_end.items():
                    df_2 = pd.read_csv("internal_table.csv")
                    with open(v["from_table"] + ".csv", '+', newline='') as f:
                        df_2[df_2[search_column_name] == values] = None   #Let the cell be null(set null)
                
                if len(foreign_end) == 0:   #go back to my table, delete node
                    for i in primary_keys_array:
                        val = tree.pop(i)
        
    elif key != None:   #schemaDict = {'table': tableName}  (no WHERE clause exists)
        #checking foreign key
        df = pd.read_csv("internal_table.csv")
        with open("internal_table.csv", 'r', newline='') as f:
            for i in df['table_name']:   #in internal table column "table name"
                if i == schemaDict['table']:
                    specific_row = df.loc[i]
                    break
        foreign_end = specific_row[5]   #Dict/find foreign_end (primary key is used by other tables)
        
        #drop_index(tree) call this function, key == primary_key
        df_1 = pd.read_csv(f"{schemaDict['table_name']}.csv")
        with open(f"{schemaDict['table_name']}.csv", 'r', newline='') as f:
            if len(foreign_end) == 0:   #no other tables are using my primary key, delete directly
                val = tree.pop(key)   #remove key and return the corresponding value
            else:   #other tables are using my primary key
                for _, v in foreign_end.items():
                    df_2 = pd.read_csv("internal_table.csv")
                    with open(v["from_table"] + ".csv", '+', newline='') as f:
                        df_2[df_2[search_column_name] == values] = None   #Let the cell be null(set null)
                
                if len(foreign_end) == 0:   #go back to my table, delete node
                    for i in primary_keys_array:
                        val = tree.pop(i)


def drop_table(schemaDict, tree):
    drop_index(tree)
    df = pd.read_csv(f"{schemaDict['table']}.csv")
    with open(f"{schemaDict['table']}.csv", 'w', newline='') as f:
        df_1 = df.drop(df.index[0:])
        

def drop_index(tree):
    #just delete root
    for key in tree.keys():
        delete_index_tree(tree=tree, key=key)
    tree.clear()
    

def search_table(Dict, tree):   #haven't finish (if search_column_name == 'primary_key')
    #Dict = table_name, column_name(要return哪些column), condition(condition_column, Like w%) (a = x)
    #'where': {'conditions': ['name LIKE ‘W%’', 'p# BETWEEN 11 AND 15', 'a_num BETWEEN 20 AND 25', 'p# IN (2,4,8)'] and "=, >, <, >=, <=, !="}
    #condition_column: need to search, column_name: need to return
    #需要回傳所有符合條件的(rows)對應的column
    txt = Dict['where']['conditions'][0]   #condition value
    search_conditions = txt.split(' ')
    search_column_name = search_conditions[0]   #get column name we need to search (only one column)
    
    #if search_column_name == 'primary_key': --> call search_index_tree()
    df_1 = pd.read_csv("internal_table.csv")
    with open("internal_table.csv", 'r', newline='') as f:   #find which column is primary_key
        for i in df_1['table_name']:
            if i == Dict['table_name']:
                specific_row = df_1.loc[i]
                break
        schema = dict(specific_row[2])   #find its schemaDict/ may fail
        primary_key_column_name = schema['primary_key']
        primary_key_type = schema['column_type'][schema['column_name'].index(primary_key_column_name)]
    
    df = pd.read_csv(f"{Dict['table_name']}.csv")
    with open(f"{Dict['table_name']}.csv", 'r', newline='') as f:
        #search_index_tree(tree, key)
        if search_column_name == primary_key_column_name:
            if search_conditions[1] == "=":
                search_value = search_conditions[2]
                if primary_key_type == "Integer":
                    search_value = int(search_value)
                row_number = search_index_tree(tree, search_value)
                data = df.iloc[row_number]
                ans = data[Dict['column_name']]
            elif search_conditions[1] == "IN":
                tmp_value = search_conditions[2]
                tmp_value = tmp_value[1:-1]
                target_values = tmp_value.split(',')
                tmp = []
                for val in target_values:
                    if primary_key_type == "Integer":
                        search_value = int(val)
                    row_number = search_index_tree(tree, search_value)
                    data = df.iloc[row_number]
                    result = data[Dict['column_name']]
                    tmp.append(result)
                ans = pd.concat(tmp)
            elif search_conditions[1] == "BETWEEN":
                search_value_1 = int(search_conditions[2])
                search_value_2 = int(search_conditions[4])
                rows = []
                for i in range(search_value_1, search_value_2+1):
                    row_number = search_index_tree(tree, i)
                    if row_number != -1:
                        rows.append(row_number)
                tmp = []
                for row in rows:
                    data = df.iloc[row]
                    result = data[Dict['column_name']]
                    tmp.append(result)
                ans = pd.concat(tmp)
            return ans
    
    if search_conditions[1] == "LIKE" or "IN" or ">" or "<" or "=" or ">=" or "<=" or "!=":
        search_value = search_conditions[2]
    elif search_conditions[1] == "BETWEEN":
        search_value_1 = int(search_conditions[2])
        search_value_2 = int(search_conditions[4])
    
    df = pd.read_csv(f"{Dict['table_name']}.csv")
    with open(f"{Dict['table_name']}.csv", 'r', newline='') as f:
        #根據condition_column,找到有那個值(ex:w%)的那個row,取出那整個row,再根據column_name看要回傳哪些對應的column的值傳回去
        if search_conditions[1] == "LIKE":   #must be string
            search_value = search_value[1:-1]   #'w%' --> w%
            ans = Like_check(df, search_column_name, search_value)
        elif search_conditions[1] == "BETWEEN":
            result = []
            for i in range(len(df[search_column_name])):
                val = int(df[search_column_name][i])
                if search_value_1 <= val and val <= search_value_2:
                    result.append(val)
            ans = pd.DataFrame(result)
        elif search_conditions[1] == "IN":   #may be string
            search_value = search_value[1:-1]
            target_values = search_value.split(',')
            tmp = []
            for val in target_values:
                result = df[df[search_column_name] == val]
                tmp.append(result)
            ans = pd.concat(tmp)
        elif search_conditions[1] == ">":
            for i in range(len(df[search_column_name])):
                val = int(df[search_column_name][i])
                ans = df[val > int(search_value)]
        elif search_conditions[1] == "<":
            for i in range(len(df[search_column_name])):
                val = int(df[search_column_name][i])
                ans = df[val < int(search_value)]
        elif search_conditions[1] == "=":   #may be string
            ans = df[df[search_column_name] == search_value]
        elif search_conditions[1] == ">=":
            for i in range(len(df[search_column_name])):
                val = int(df[search_column_name][i])
                ans = df[val >= int(search_value)]
        elif search_conditions[1] == "<=":
            for i in range(len(df[search_column_name])):
                val = int(df[search_column_name][i])
                ans = df[val <= int(search_value)]
        elif search_conditions[1] == "!=":   #may be string
            for i in range(len(df[search_column_name])):
                val = int(df[search_column_name][i])
                ans = df[val != int(search_value)]
    return ans


def search_index_tree(tree, key):
    if tree.has_key(key) is true:
        row_number = tree.get(key[, default=None])
    else:
        row_number = -1   #Not Found!!!!!
    return row_number
    

def use():   #define which database to use
    pass
    
    
def print_tree():   #print iteritems()
    pass


if __name__ == '__main__':   #need to be deleted (just for testing)
    schemaDict = {
        "table_name": "ABC",
        "primary_key": ["name_x1", "name_x2"],
        "column_name": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "column_type": ["integer", "string", "integer", "integer"],
        "foreign_key": ["name_x3", "name_x4"],
        "foreign_table": ["T_1", "T_2"],
        "foreign_column": ["C_1", "C_2"],
        "foreign_delete": ["S_1", "S_2"]
    }
    
    schemaDict2 = {   #need to deal with index tree's location in execution step
        "table_name": "Table",
        "primary_key": "table_name",
        "column_name": ["table_name", "schemaDict", "length", "foreign_start", "foreign_end"],
        "column_type": ["string", "dictionary", "integer", "dictionary", "dictionary"]
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
