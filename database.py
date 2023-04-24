from BTrees.OIBTree import OIBTree
import csv
import pandas as pd
import ast
import re


def readDict(schemaDict): 
    return ast.literal_eval(schemaDict)


def LIKE(str): #Change Like clause conndition , w%, into regular expression. w%=all string that starts with w. w_, w+any one character
#https://www.w3schools.com/sql/sql_like.asp 
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
    
    
def Like_check(df, column_name, condition): #str = LIKE's condition
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
    #print(table)
    table.to_csv("internal_table.csv", index=False)
    

def insert_internal_table(row):   #parameter should be a Dataframe
    df = pd.read_csv("internal_table.csv")
    #print(df)
    #print(row)
    with open("internal_table.csv", 'a', newline='') as f:
        new_row = pd.concat([df,row], ignore_index=True)
        #print(new_row)
        new_row.to_csv("internal_table.csv", index=False)


def create_table(schemaDict):   #parameter is a dictionary
    column_names = schemaDict.get('column_name', [])
    table = pd.DataFrame(columns=column_names)
    #print(table)
    '''
        "table_name": "ABC",
        "primary_key": ["name_x2"],
        "column_name": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "column_type": ["integer", "string", "integer", "integer"],
        "foreign_key": ["name_x3", "name_x4"],
        "foreign_table": ["T_1", "T_2"],
        "foreign_column": ["C_1", "C_2"],
        "foreign_delete": ["S_1", "S_2"]
        
        "column_name": ["table_name", "schemaDict", "length", "foreign_start", "foreign_end"]
    '''
    table.to_csv(f"{schemaDict['table_name']}.csv", index=False)
    df = pd.read_csv("internal_table.csv")
    with open("internal_table.csv", 'a', newline='') as f:
        this_table_name = schemaDict['table_name']
        length = 0
        foreign_start = {}   #from this table point to other table
        foreign_end = {}   #the target table (have foreign key) --> it's empty at the start
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
            foreign_end_element = readDict(modify_row['foreign_end'])
            foreign_tmp = {}
            foreign_tmp['my_primary_column_name'] = schemaDict['foreign_column'][i]
            foreign_tmp['from_table'] = schemaDict['table_name']
            foreign_tmp['foreign_column_name'] = schemaDict['foreign_key'][i]
            foreign_end_element[schemaDict['table_name']] = foreign_tmp
            modify_row['foreign_end'] = foreign_end_element
            df.loc[df['table_name'] == foreign_table_name] = modify_row
            df.to_csv("internal_table.csv", index=False)
        
        #write to a new row, then insert into internal table
        new_dict = {"table_name":[this_table_name], "schemaDict":[str(schemaDict)], "length":[length], "foreign_start":[str(foreign_start)], "foreign_end":[str(foreign_end)]}
        row = pd.DataFrame(new_dict)
        insert_internal_table(row)
        tree = create_index_tree()
    return tree


def create_index_tree():
    tree = OIBTree()
    return tree


def insert_table(schemaDict, tree):
    #schemaDict = {'table': tableName, 'columns': columns, 'values': values}
    #check for duplicate on primary key, if existing --> return Error
    df = pd.read_csv("internal_table.csv")
    df_1 = pd.read_csv(f"{schemaDict['table']}.csv")
    with open("internal_table.csv", 'r', newline='') as f:   #find which column is primary_key
        specific_row = df[df['table_name'] == schemaDict['table']]
        #for i in df['table_name']:
            #if i == schemaDict['table']:
                #specific_row = df.loc[i]
                #break
        
        #print(specific_row.loc[:,"schemaDict"].to_string(index=False))
        #tmp = specific_row.loc[:,"schemaDict"].to_string(index=False)
        #print(tmp)
        #schema = readDict(str(specific_row.loc[:,"schemaDict"].values)[1:-1])   #find its schemaDict
        schema = readDict(specific_row.loc[:,"schemaDict"].to_string(index=False))
        #print(type(schema))
        primary_key_column_name = schema['primary_key']
    
    #print(primary_key_column_name)
    index = -1
    for i in range(len(schemaDict['columns'])):
        if primary_key_column_name[0] == schemaDict['columns'][i]:
            #print(schemaDict['columns'][i])
            index = i
            break
    #print(df_1)
    #print(df_1.loc[primary_key_column_name[0]])
    if not (df_1.empty) and index != -1 and schemaDict['values'][index] in df_1.loc[primary_key_column_name[0]]:
        print("Error!!!!!")
    
    with open("internal_table.csv", 'a+', newline='') as f:
        specific_row = df[df['table_name'] == schemaDict['table']]
        length = int(specific_row.loc[:,"length"].to_string(index=False))   #find its length
        length += 1
        #print(length)
        row = df.index[df['table_name'] == schemaDict['table']].to_list()[0]
        #specific_row.loc[:,"length"] = length
        #print(df.loc[schemaDict['table'],"length"])
        df[df['table_name'] == schemaDict['table']] = specific_row
        df.loc[row,"length"] = length
        df.to_csv("internal_table.csv", index=False)
        
    with open(f"{schemaDict['table']}.csv", 'a', newline='') as f:
        #row = df_1.append(pd.Series(schemaDict['values'], index=df_1.columns.values))
        #print(schemaDict['columns'])
        #new_row = pd.concat([df,row], ignore_index=True)
        row = pd.concat([df_1,pd.DataFrame([schemaDict['values']], columns=schemaDict['columns'])], ignore_index=True)
        #print(row)
        row.to_csv(f"{schemaDict['table']}.csv", index=False)
    
    tree = insert_index_tree(tree, schemaDict['values'][index], length-1)   #tree, key, value
    return tree
    

def insert_index_tree(tree, key, row_number):
    #user can also delete index tree (but we don't want the index tree to be deleted) --> when executing, avoid any index to be called index tree name
    #every node stores a key-value pair (primary key, row_number)
    #this is also used in updating index tree (automatically)
    tree.insert(key, row_number)
    return tree


def update_table(tree, schemaDict, orig_key):
    #schemaDict = {'table': tableName, 'columns': columns, 'values': values}
    row_number = -1
    df = pd.read_csv(f"{schemaDict['table']}.csv")
    #print(df.loc[0])
    with open(f"{schemaDict['table']}.csv", 'a+', newline='') as f:
        #new_row = pd.DataFrame(pd.Series(schemaDict['values'], index=df.columns.values))
        new_row = pd.DataFrame(schemaDict['values'])
        new_row = new_row.set_index(pd.Series(schemaDict['columns']))
        #print(new_row)
        row_number = search_index_tree(tree, orig_key)
        if row_number == None:
            return None
        #print("row_number: ", row_number)
        #print(new_row.to_dict()[0])
        df.loc[row_number] = new_row.to_dict()[0]
    #print(new_row.to_dict()[row_number])
    df.to_csv(f"{schemaDict['table']}.csv", index=False)
    
    #if modify original primary key --> need to delete the node and insert a new node
    df_1 = pd.read_csv("internal_table.csv")
    with open("internal_table.csv", 'r', newline='') as f:
        #print("kkkkkkkkkkkkkkk")
        specific_row = df_1[df_1['table_name'] == schemaDict['table']]
        schema = readDict(specific_row.loc[:,"schemaDict"].to_string(index=False))
        primary_key_column_name = schema['primary_key'][0]   #since we only have one primary key
    #print(type(primary_key_column_name))
    index = -1
    for i in range(len(schemaDict['columns'])):
        if primary_key_column_name == schemaDict['columns'][i]:
            #print("zzzzzzzzzzzzz")
            index = i
            break
    #print(index)
    if index != -1 and schemaDict['values'][i] != orig_key:
        #print("zzzzzzzzzzzzz")
        delete_index_tree(tree, orig_key, schemaDict['table'])
        key = schemaDict['values'][i]
        insert_index_tree(tree, key, row_number)
    return tree
     
    
def delete_index_tree(tree, key, table_name):
    #csv file do not involve delete, just delete the node in index tree (only set null)
    #if key!=None, key = primary, 一開始check foreign_end: if empty->delete directly; else not empty: 去其他表刪(把那一個cell set null),再回來就可以直接刪index tree node--> pop()
    #schemaDict = {'table': tableName}
    
    #checking foreign key
    df = pd.read_csv("internal_table.csv")
    with open("internal_table.csv", 'r', newline='') as f:
        specific_row = df[df['table_name'] == table_name]
    foreign_end = readDict(specific_row.loc[:,"foreign_end"].to_string(index=False))   #Dict/find foreign_end (primary key is used by other tables)
    
    #drop_index(tree) call this function, key == primary_key
    df_1 = pd.read_csv(table_name + ".csv")
    with open(table_name + ".csv", 'r', newline='') as f:
        if len(foreign_end) == 0:   #no other tables are using my primary key, delete directly
            val = tree.pop(key)   #remove key and return the corresponding value
        else:   #other tables are using my primary key
            for _, v in foreign_end.items():
                df_2 = pd.read_csv("internal_table.csv")
                with open(v["from_table"] + ".csv", '+', newline='') as f:
                    df_2[v['foreign_column_name'] == key] = None   #Let the cell be null(set null)
            
            if len(foreign_end) == 0:   #go back to my table, delete node
                val = tree.pop(i)
    return tree


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
    

def search_table(Dict, tree):
    #Dict = table_name, column_name(要return哪些column), condition(condition_column, Like w%) (a = x)
    #Dict = {
    # 'table_name' = table_name
    # 'column_name' = column_1, column_2, .... essentially the columns needed for select
    # 'where': {condition: condition statement}
    #}
    #'where': {'condition': ['name LIKE ‘W%’', 'p# BETWEEN 11 AND 15', 'a_num BETWEEN 20 AND 25', 'p# IN (2,4,8)'] and "=, >, <, >=, <=, !="}
    #condition_column: need to search, column_name: need to return
    #需要回傳所有符合條件的(rows)對應的column
    txt = Dict['where']['condition'][0]   #condition value
    search_conditions = txt.split(' ')
    search_column_name = search_conditions[0]   #get column name we need to search (only one column)
    
    #if search_column_name == 'primary_key': --> call search_index_tree()
    df_1 = pd.read_csv("internal_table.csv")
    with open("internal_table.csv", 'r', newline='') as f:   #find which column is primary_key
        specific_row = df_1[df_1['table_name'] == Dict['table_name']]
        schema = readDict(specific_row.loc[:,"schemaDict"].to_string(index=False))
        #schema = readDict(specific_row[2])   #find its schemaDict/ may fail
        primary_key_column_name = schema['primary_key'][0]
        #primary_key_column_name = schema['primary_key']
        primary_key_type = schema['column_type'][schema['column_name'].index(primary_key_column_name)]
        column_name = schema['column_name']
    
    ans = pd.DataFrame()
    return_columns = Dict['column_name']
    flag = False
    df = pd.read_csv(f"{Dict['table_name']}.csv")
    with open(f"{Dict['table_name']}.csv", 'r', newline='') as f:
        #search_index_tree(tree, key)
        if search_column_name == primary_key_column_name:   #primary key --> use index tree to accelerate searching
            #print("kkkkkkkkkkk")
            if search_conditions[1] == "=":
                flag = True
                search_value = search_conditions[2]
                if primary_key_type == "Integer":
                    search_value = int(search_value)
                row_number = search_index_tree(tree, search_value)
                if row_number == None:
                    ans = pd.Series()
                else:
                    data = df.iloc[row_number]
                    ans = data[Dict['column_name']]
            elif search_conditions[1] == "IN":
                flag = True
                tmp_value = search_conditions[2]
                tmp_value = tmp_value[1:-1]
                target_values = tmp_value.split(',')
                #print(target_values)
                tmp = pd.DataFrame()
                #print(df.dtypes)
                for val in target_values:
                    #print(val[1:-1])
                    if primary_key_type == "Integer":
                        val = int(val)
                        row_number = search_index_tree(tree, val)
                    else:
                        #print(type(str(val[1:-1])))
                        row_number = search_index_tree(tree, str(val[1:-1]))
                        
                    if row_number == None:
                        continue
                    #print(tree.has_key(val))
                    tmp = df.iloc[row_number].to_dict()
                    tmp = 
                    #print(data)
                    #result = data[Dict['column_name']]
                    #tmp.append(data)
                print(tmp)
                if len(tmp) == 0:
                    ans = pd.DataFrame()
                else:
                    ans = tmp
            elif search_conditions[1] == "BETWEEN":
                flag = True
                search_value_1 = int(search_conditions[2])
                search_value_2 = int(search_conditions[4])
                rows = []
                for i in range(search_value_1, search_value_2+1):
                    row_number = search_index_tree(tree, i)
                    if row_number is not None:
                        rows.append(row_number)
                tmp = []
                for row in rows:
                    data = df.iloc[row]
                    #result = data[Dict['column_name']]
                    #tmp.append(result)
                    #print(data)
                    tmp.append(data)
                ans = pd.concat(tmp)
            #print("-------")
            #print(ans)
            if flag == True:
                if ans.empty:   #no row match this condition
                    return None
                else:
                    if return_columns[0] == "*":   #need every column
                        return ans
                    return ans.loc[:,return_columns]
    
    if search_conditions[1] == "LIKE" or search_conditions[1] == "IN" or search_conditions[1] == ">" or search_conditions[1] == "<" or search_conditions[1] == "=" or search_conditions[1] == ">=" or search_conditions[1] == "<=" or search_conditions[1] == "!=":
        search_value = search_conditions[2]
    elif search_conditions[1] == "BETWEEN":
        #print("kkkkkkkkkkk")
        search_value_1 = int(search_conditions[2])
        search_value_2 = int(search_conditions[4])
    
    df = pd.read_csv(f"{Dict['table_name']}.csv")
    with open(f"{Dict['table_name']}.csv", 'r', newline='') as f:
        #print("kkkkkkkkkkk")
        #根據condition_column,找到有那個值(ex:w%)的那個row,取出那整個row,再根據column_name看要回傳哪些對應的column的值傳回去
        if search_conditions[1] == "LIKE":   #must be string
            search_value = search_value[1:-1]   #'w%' --> w%
            ans = Like_check(df, search_column_name, search_value)
        elif search_conditions[1] == "BETWEEN":
            #print("kkkkkkkkkkk")
            result = []
            for i in range(len(df[search_column_name])):
                val = int(df[search_column_name][i])
                if search_value_1 <= val and val <= search_value_2:
                    result.append(df.iloc[i])
            #print(result)
            ans = pd.DataFrame(result)
        elif search_conditions[1] == "IN":   #may be string
            search_value = search_value[1:-1]
            target_values = search_value.split(',')
            tmp = []
            for val in target_values:
                #print(df.dtypes[search_column_name] == "int64")
                if df.dtypes[search_column_name] == "int64":
                    val = int(val)
                #print(df[search_column_name])
                #print(df[search_column_name] == val)
                result = df[df[search_column_name] == val]
                tmp.append(result)
                #print(type(val))
            #print(tmp)
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
    
    if ans.empty:   #no row match this condition
        return None
    else:
        if return_columns[0] == "*":   #need every column
            return ans
        return ans.loc[:,return_columns]


def search_index_tree(tree, key):
    #for i in tree.iteritems():
        #print(i)
    #print(key)
    if tree.has_key(key) == True:
        #print(key)
        row_number = tree.get(key)
    else:
        row_number = None   #Not Found!!!!!
    return row_number
    

def use():   #define which database to use
    pass
    
    
def print_tree():   #print iteritems()
    pass

'''
if __name__ == '__main__':   #need to be deleted (just for testing)
    schemaDict = { #CT
        "table_name": "ABC",
        "primary_key": ["name_x2"],
        "column_name": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "column_type": ["integer", "string", "integer", "integer"],
        "foreign_key": ["name_x3", "name_x4"],
        "foreign_table": ["T_1", "T_2"],
        "foreign_column": ["C_1", "C_2"],
        "foreign_delete": ["S_1", "S_2"]
    }
    
    schemaDict2 = {   #need to deal with index tree's location in execution step. The schema 
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
'''
Rel-i-i-10
10 tuples: {<1, 1>, <2, 2>, <3, 3>, …, <10,10>}

primary key | value
1              1
2              2
3              3
4              4
5              5
6              6
7              7
8              8
9              9
10             10
'''


'''
f = open(, "w")
then build index tree
每塞一行進csv file加一個node (table & tree build simutaneously)

用什麼格式塞進csv (先試著塞list在一行，讀出來也是list)
需要另存每個column的original type (create table時會用到)
可放在Bplustree最前面

從csv拉出來全為string
'''
#Fix delete by removing the search part --> okay
#Update table  --> okay
#Handle how store dictionary --> okay
#write test cases
