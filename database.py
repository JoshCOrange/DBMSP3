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


def create_internal_table():
    schemaDict = {
        "table_name": "Internal_table",
        "primary_key": "table_name",
        "column_name": ["table_name", "schemaDict", "length", "foreign_start", "foreign_end"],
        "column_type": ["string", "dictionary", "integer", "dictionary", "dictionary"]
    }
    column_names = schemaDict.get('column_name', [])
    table = pd.DataFrame(columns=column_names)
    table.to_csv("internal_table.csv", index=False)
    

def insert_internal_table(row):   #parameter should be a Dataframe
    df = pd.read_csv("internal_table.csv")
    with open("internal_table.csv", 'a', newline='') as f:
        new_row = pd.concat([df,row], ignore_index=True)
        new_row.to_csv("internal_table.csv", index=False)


def create_table(schemaDict):   #parameter is a dictionary
    column_names = schemaDict.get('column_names', [])
    table = pd.DataFrame(columns=column_names)
    '''
        "table_name": "ABC",
        "primary_key": ["name_x2"],
        "column_names": ["name_x1", "name_x2", "name_x3", "name_x4"],
        "column_types": ["integer", "string", "integer", "integer"],
        "foreign_keys": ["name_x3", "name_x4"],
        "foreign_tables": ["T_1", "T_2"],
        "foreign_columns": ["C_1", "C_2"],
        "foreign_deletes": ["S_1", "S_2"]
        
        "column_name": ["table_name", "schemaDict", "length", "foreign_start", "foreign_end"]
    '''
    table.to_csv(f"{schemaDict['table_name']}.csv", index=False)
    df = pd.read_csv("internal_table.csv")
    with open("internal_table.csv", 'a', newline='') as f:
        this_table_name = schemaDict['table_name']
        length = 0
        foreign_start = {}   #from this table (has foreign key) point to other table
        foreign_end = {}   #target table whose primary key is others foreign key --> it's empty at the start
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
        for i in range(len(schemaDict['foreign_keys'])):   #fill foreign_start
            tmp = {}
            tmp['foreign_column_name'] = schemaDict['foreign_keys'][i]
            tmp['point_to_table'] = schemaDict['foreign_tables'][i]
            tmp['primary_column_name'] = schemaDict['foreign_columns'][i]
            foreign_start[schemaDict['foreign_tables'][i]] = tmp
            
            #modify foreign_end of foreign tables
            foreign_table_name = schemaDict['foreign_tables'][i]
            df = df.set_index("table_name")
            modify_row = df.loc[foreign_table_name]
            if modify_row['foreign_end'] != "{}":
                foreign_end_element = readDict(modify_row.loc[:,'foreign_end'])
            else:
                foreign_end_element = {}
            foreign_tmp = {}
            foreign_tmp['my_primary_column_name'] = schemaDict['foreign_columns'][i]
            foreign_tmp['from_table'] = schemaDict['table_name']
            foreign_tmp['foreign_column_name'] = schemaDict['foreign_keys'][i]
            foreign_end_element.update({schemaDict['table_name']: foreign_tmp})
            df.loc[foreign_table_name,"foreign_end"] = str(foreign_end_element)
            df = df.reset_index()
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
    #schemaDict = {'table_name': tableName, 'columns': columns, 'values': values}
    #check for duplicate on primary key, if existing --> return Error
    df = pd.read_csv("internal_table.csv")
    df_1 = pd.read_csv(f"{schemaDict['table_name']}.csv")
    with open("internal_table.csv", 'r', newline='') as f:   #find which column is primary_key
        specific_row = df[df['table_name'] == schemaDict['table_name']]
        schema = readDict(specific_row.loc[:,"schemaDict"].to_string(index=False))
        primary_key_column_name = schema['primary_key'][0]
        primary_key_type = schema['column_types'][schema['column_names'].index(primary_key_column_name)]
    
    index = -1
    for i in range(len(schemaDict['columns'])):
        if primary_key_column_name == schemaDict['columns'][i]:
            index = i
            break
    '''
    if not (df_1.empty) and index != -1 and schemaDict['values'][index] in df_1.loc[primary_key_column_name[0]]:   #check for duplicate primary key
        print("Error!!!!!")
    '''
    
    with open("internal_table.csv", 'a+', newline='') as f:
        specific_row = df[df['table_name'] == schemaDict['table_name']]
        length = int(specific_row.loc[:,"length"].to_string(index=False))   #find its length
        length += 1
        row = df.index[df['table_name'] == schemaDict['table_name']].to_list()[0]
        df[df['table_name'] == schemaDict['table_name']] = specific_row
        df.loc[row,"length"] = length
        df.to_csv("internal_table.csv", index=False)
        
    with open(f"{schemaDict['table_name']}.csv", 'a', newline='') as f:
        row = pd.concat([df_1,pd.DataFrame([schemaDict['values']], columns=schemaDict['columns'])], ignore_index=True)
        row.to_csv(f"{schemaDict['table_name']}.csv", index=False)
    if primary_key_type == "integer":
        schemaDict['values'][index] = int(schemaDict['values'][index])
        
    insert_index_tree(tree, schemaDict['values'][index], length-1)   #tree, key, value
    

def insert_index_tree(tree, key, row_number):
    #user can also delete index tree (but we don't want the index tree to be deleted) --> when executing, avoid any index to be called index tree name
    #every node stores a key-value pair (primary key, row_number)
    #this is also used in updating index tree (automatically)
    tree.insert(key, row_number)
    return tree


def update_table(tree, schemaDict, orig_key):
    #schemaDict = {'table_name': tableName, 'columns': columns, 'values': values}
    df_1 = pd.read_csv("internal_table.csv")
    with open("internal_table.csv", 'r', newline='') as f:
        specific_row = df_1[df_1['table_name'] == schemaDict['table_name']]
        schema = readDict(specific_row.loc[:,"schemaDict"].to_string(index=False))
        primary_key_column_name = schema['primary_key'][0]   #since we only have one primary key
    
    index = -1
    for i in range(len(schemaDict['columns'])):
        if primary_key_column_name == schemaDict['columns'][i]:
            index = i
            break
    
    if index != -1 and schemaDict['values'][index] != orig_key:
        #check for foreign key relationship (check my foreign_end, if not empty --> cannot update)
        df_1 = pd.read_csv("internal_table.csv")
        with open("internal_table.csv", 'r', newline='') as f:
            specific_row = df_1[df_1['table_name'] == schemaDict['table_name']]
        foreign_end = readDict(specific_row.loc[:,"foreign_end"].to_string(index=False))
        if len(foreign_end) != 0:
            raise Exception("Cannot update due to the existence of foreign key!")
    
    row_number = -1
    df = pd.read_csv(f"{schemaDict['table_name']}.csv")
    with open(f"{schemaDict['table_name']}.csv", 'a+', newline='') as f:
        new_row = pd.DataFrame(schemaDict['values'])
        new_row = new_row.set_index(pd.Series(schemaDict['columns']))
        row_number = search_index_tree(tree, orig_key)
        if row_number == None:
            return None
        df.loc[row_number] = new_row.to_dict()[0]
    df.to_csv(f"{schemaDict['table_name']}.csv", index=False)
    
    #if modify original primary key --> need to delete the node and insert a new node
    df_1 = pd.read_csv("internal_table.csv")
    with open("internal_table.csv", 'r', newline='') as f:
        specific_row = df_1[df_1['table_name'] == schemaDict['table_name']]
        schema = readDict(specific_row.loc[:,"schemaDict"].to_string(index=False))
        primary_key_column_name = schema['primary_key'][0]   #since we only have one primary key
    index = -1
    for i in range(len(schemaDict['columns'])):
        if primary_key_column_name == schemaDict['columns'][i]:
            index = i
            break

    if index != -1 and schemaDict['values'][index] != orig_key:
        delete_index_tree(tree, orig_key, schemaDict['table_name'])
        key = schemaDict['values'][index]
        insert_index_tree(tree, key, row_number)
    return tree
     
    
def delete_index_tree(tree, key, table_name):
    #csv file do not involve delete, just delete the node in index tree (only set null)
    #key = primary key, 一開始check foreign_end: if empty->delete directly; else not empty: 去其他表刪(把那一個cell set null),再回來就可以直接刪index tree node--> pop()
    #schemaDict = {'table_name': tableName}
    #checking foreign key
    df = pd.read_csv("internal_table.csv")
    with open("internal_table.csv", 'r', newline='') as f:
        specific_row = df[df['table_name'] == table_name]
    foreign_end = readDict(specific_row.loc[:,"foreign_end"].to_string(index=False))   #Dict/find foreign_end (primary key is used by other tables)
    
    #drop_index(tree) call this function, key == primary_key
    if len(foreign_end) == 0:   #no other tables are using my primary key, delete directly
        val = tree.pop(key)   #remove key and return the corresponding value
    else:   #other tables are using my primary key
        for _, v in foreign_end.items():
            df_1 = pd.read_csv("internal_table.csv")
            with open("internal_table.csv", 'r', newline='') as f:   #find which column is primary_key
                specific_row = df_1[df_1['table_name'] == v["from_table"]]
                schema = readDict(specific_row.loc[:,"schemaDict"].to_string(index=False))
                primary_key_column_name = schema['primary_key'][0] #foreign table(arrow start)'s primary column
            
            df_2 = pd.read_csv(v["from_table"] + ".csv")
            with open(v["from_table"] + ".csv", 'a+', newline='') as f:
                col = df_2[df_2.loc[:,[v['foreign_column_name']]] != key][v['foreign_column_name']]
                df_2[v['foreign_column_name']] = col
            df_2.to_csv(v["from_table"] + ".csv", index=False)
        val = tree.pop(key)   #go back to my table, delete node
    
    return tree


def drop_table(schemaDict, tree):
    my_table_name = schemaDict['table_name']
    drop_index(tree, my_table_name)
    
    #check foreign key relationship (delete the dictionaries)
    '''
    foreign_start = {"table_name": {"foreign_column_name": val_1, "point_to_table": val_2,
                    "primary_column_name": val_3},
                    "table_name": {"foreign_column_name": val_4, "point_to_table": val_5,
                    "primary_column_name": val_6}}
                    
    foreign_end = {"table_name": {"my_primary_column_name": val_1, "from_table": val_2,
                   "foreign_column_name": val_3},
                   "table_name": {"my_primary_column_name": val_4, "from_table": val_5,
                   "foreign_column_name": val_6},}
    '''
    df_2 = pd.read_csv("internal_table.csv")
    with open("internal_table.csv", 'r', newline='') as f:
        specific_row = df_2[df_2['table_name'] == my_table_name]
    my_foreign_start = readDict(specific_row.loc[:,"foreign_start"].to_string(index=False))
    my_foreign_end = readDict(specific_row.loc[:,"foreign_end"].to_string(index=False))
    
    if len(my_foreign_start) != 0:   #need to delete other tables' foreign_end (in internal table)
        #先開internal table,根據自己的foreign_start裡面的key(table_name/實際為foreign table),找到那幾張擁有primary key的tables. 然後一行一行抓出那幾張table的row,去他們的foreign_end delete掉這張table的dictionary(用pop(key))
        df_2 = pd.read_csv("internal_table.csv")
        df_2 = df_2.set_index("table_name")
        with open("internal_table.csv", 'a+', newline='') as f:
            other_table_name_list = my_foreign_start.keys()
            for other_table_name in other_table_name_list:
                other_table_data_row = df_2.loc[other_table_name]
                other_foreign_end = readDict(other_table_data_row.loc["foreign_end"])
                other_foreign_end.pop(my_table_name)
                df_2.loc[other_table_name,"foreign_end"] = str(other_foreign_end)
            df_2 = df_2.reset_index()
            df_2.to_csv("internal_table.csv", index=False)
    
    if len(my_foreign_end) != 0:   #need to delete other tables' foreign_start (in internal table)
        #先開internal table,根據自己的foreign_end裡面的key(table_name/實際為foreign table),找到那幾張擁有foreign key的tables. 然後一行一行抓出那幾張table的row,去他們的foreign_start delete掉這張table的dictionary(pop(key))
        df_2 = pd.read_csv("internal_table.csv")
        df_2 = df_2.set_index("table_name")
        with open("internal_table.csv", 'a+', newline='') as f:
            other_table_name_list = my_foreign_end.keys()
            for other_table_name in other_table_name_list:
                other_table_data_row = df_2.loc[other_table_name]
                other_foreign_start = readDict(other_table_data_row.loc["foreign_start"])
                other_foreign_start.pop(my_table_name)
                df_2.loc[other_table_name,"foreign_start"] = str(other_foreign_start)
            df_2 = df_2.reset_index()
            df_2.to_csv("internal_table.csv", index=False)
    #everything is done, now drop my csv file
    df = pd.read_csv(f"{schemaDict['table_name']}.csv")
    with open(f"{schemaDict['table_name']}.csv", 'w', newline='') as f:
        df_1 = df.drop(df.index[0:])
    

def drop_index(tree, table_name):
    #just delete root
    for key in tree.keys():
        delete_index_tree(tree=tree, key=key, table_name=table_name)
    tree.clear()
    

def search_table(Dict, tree):
    #Dict = table_name, column_name(要return哪些column), condition(condition_column, Like w%) (a = x)
    #Dict = {
    # 'table_name': table_name
    # 'column_name': column_1, column_2, .... essentially the columns needed for select
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
        primary_key_column_name = schema['primary_key'][0]
        primary_key_type = schema['column_types'][schema['column_names'].index(primary_key_column_name)]
        column_name = schema['column_names']
    
    ans = pd.DataFrame()
    return_columns = Dict['column_name']
    flag = False
    df = pd.read_csv(f"{Dict['table_name']}.csv")
    with open(f"{Dict['table_name']}.csv", 'r', newline='') as f:
        #search_index_tree(tree, key)
        if search_column_name == primary_key_column_name:   #primary key --> use index tree to accelerate searching
            if search_conditions[1] == "=":
                flag = True
                tmp = pd.DataFrame()
                search_value = search_conditions[2]
                if primary_key_type == "integer":
                    search_value = int(search_value)
                elif primary_key_type == "string":
                    search_value = search_value[1:-1]
                row_number = search_index_tree(tree, search_value)
                if row_number == None:
                    ans = pd.DataFrame()
                else:
                    tmp = df.iloc[row_number].to_dict()
                    tmp = pd.DataFrame.from_dict(tmp, orient="index").T
                    ans = tmp
            elif search_conditions[1] == "IN":
                flag = True
                tmp_value = search_conditions[2]
                tmp_value = tmp_value[1:-1]
                target_values = tmp_value.split(',')
                tmp = pd.DataFrame()
                result = pd.DataFrame()
                for val in target_values:
                    if primary_key_type == "integer":
                        val = int(val.strip())
                        row_number = search_index_tree(tree, val)
                    else:
                        row_number = search_index_tree(tree, str(val.strip()[1:-1]))
                        
                    if row_number == None:
                        continue
                        
                    tmp = df.iloc[row_number].to_dict()
                    tmp = pd.DataFrame.from_dict(tmp, orient="index").T
                    if result.empty:
                        result = tmp
                    else:
                        result = pd.concat([result, tmp])

                if len(tmp) == 0:
                    ans = pd.DataFrame()
                else:
                    ans = result
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
                    tmp.append(data)
                ans = pd.DataFrame(tmp)

            if flag == True:
                if ans.empty:   #no row match this condition
                    return None
                else:
                    return ans
    
    if search_conditions[1] == "LIKE" or search_conditions[1] == "IN" or search_conditions[1] == ">" or search_conditions[1] == "<" or search_conditions[1] == "=" or search_conditions[1] == ">=" or search_conditions[1] == "<=" or search_conditions[1] == "!=":
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
                    result.append(df.iloc[i])
            ans = pd.DataFrame(result)
        elif search_conditions[1] == "IN":   #may be string
            search_value = search_value[1:-1]
            target_values = search_value.split(',')
            tmp = []
            for val in target_values:
                if df.dtypes[search_column_name] == "int64":
                    val = int(val.strip())
                else:
                    val = str(val.strip()[1:-1])
                result = df[df[search_column_name] == val]
                tmp.append(result)
            ans = pd.concat(tmp)
        elif search_conditions[1] == ">":
            tmp = []
            result = pd.DataFrame()
            for i in range(len(df[search_column_name])):
                val = df[search_column_name][i]
                if val > int(search_value):
                    tmp = df.iloc[i].to_dict()
                    tmp = pd.DataFrame.from_dict(tmp, orient="index").T
                    if result.empty:
                        result = tmp
                    else:
                        result = pd.concat([result, tmp])
            ans = result
        elif search_conditions[1] == "<":
            tmp = []
            result = pd.DataFrame()
            for i in range(len(df[search_column_name])):
                val = df[search_column_name][i]
                if val < int(search_value):
                    tmp = df.iloc[i].to_dict()
                    tmp = pd.DataFrame.from_dict(tmp, orient="index").T
                    if result.empty:
                        result = tmp
                    else:
                        result = pd.concat([result, tmp])
            ans = result
        elif search_conditions[1] == "=":   #may be string
            if df.dtypes[search_column_name] == "int64":
                search_value = int(search_value)
            ans = df[df[search_column_name] == search_value]
        elif search_conditions[1] == ">=":
            tmp = []
            result = pd.DataFrame()
            for i in range(len(df[search_column_name])):
                val = df[search_column_name][i]
                if val >= int(search_value):
                    tmp = df.iloc[i].to_dict()
                    tmp = pd.DataFrame.from_dict(tmp, orient="index").T
                    if result.empty:
                        result = tmp
                    else:
                        result = pd.concat([result, tmp])
            ans = result
        elif search_conditions[1] == "<=":
            tmp = []
            result = pd.DataFrame()
            for i in range(len(df[search_column_name])):
                val = df[search_column_name][i]
                if val <= int(search_value):
                    tmp = df.iloc[i].to_dict()
                    tmp = pd.DataFrame.from_dict(tmp, orient="index").T
                    if result.empty:
                        result = tmp
                    else:
                        result = pd.concat([result, tmp])
            ans = result
        elif search_conditions[1] == "!=":   #may be string
            tmp = []
            result = pd.DataFrame()
            for i in range(len(df[search_column_name])):
                val = df[search_column_name][i]
                if df.dtypes[search_column_name] == "int64" and val != int(search_value):
                    tmp = df.iloc[i].to_dict()
                    tmp = pd.DataFrame.from_dict(tmp, orient="index").T
                    if result.empty:
                        result = tmp
                    else:
                        result = pd.concat([result, tmp])
                elif df.dtypes[search_column_name] == "object" and val != search_value[1:-1]:
                    tmp = df.iloc[i].to_dict()
                    tmp = pd.DataFrame.from_dict(tmp, orient="index").T
                    if result.empty:
                        result = tmp
                    else:
                        result = pd.concat([result, tmp])
            ans = result
    
    if ans.empty:   #no row match this condition
        return None
    else:
        return ans


def search_table_having(Dict):
    #Dict={
        #'df': df,
        #'column_name': column (return column)
        #'having': {condition: condition statement}
    #}
    txt = Dict['having']['condition'][0]   #condition value
    search_conditions = txt.split(' ')
    search_column_name = search_conditions[0]   #get column name we need to search (only one column)
    df = Dict['df']
    if search_conditions[1] == "LIKE" or search_conditions[1] == "IN" or search_conditions[1] == ">" or search_conditions[1] == "<" or search_conditions[1] == "=" or search_conditions[1] == ">=" or search_conditions[1] == "<=" or search_conditions[1] == "!=":
        search_value = search_conditions[2]
    elif search_conditions[1] == "BETWEEN":
        search_value_1 = int(search_conditions[2])
        search_value_2 = int(search_conditions[4])
    
    if search_conditions[1] == "LIKE":   #must be string
        search_value = search_value[1:-1]   #'w%' --> w%
        ans = Like_check(df, search_column_name, search_value)
    elif search_conditions[1] == "BETWEEN":
        result = []
        for i in range(len(df[search_column_name])):
            val = int(df[search_column_name][i])
            if search_value_1 <= val and val <= search_value_2:
                result.append(df.iloc[i])
        ans = pd.DataFrame(result)
    elif search_conditions[1] == "IN":   #may be string
        search_value = search_value[1:-1]
        target_values = search_value.split(',')
        tmp = []
        for val in target_values:
            if df.dtypes[search_column_name] == "int64":
                val = int(val.strip())
            else:
                val = str(val.strip()[1:-1])
            result = df[df[search_column_name] == val]
            tmp.append(result)
        ans = pd.concat(tmp)
    elif search_conditions[1] == ">":
        tmp = []
        result = pd.DataFrame()
        for i in range(len(df[search_column_name])):
            val = df[search_column_name][i]
            if val > int(search_value):
                tmp = df.iloc[i].to_dict()
                tmp = pd.DataFrame.from_dict(tmp, orient="index").T
                if result.empty:
                    result = tmp
                else:
                    result = pd.concat([result, tmp])
        ans = result
    elif search_conditions[1] == "<":
        tmp = []
        result = pd.DataFrame()
        for i in range(len(df[search_column_name])):
            val = df[search_column_name][i]
            if val < int(search_value):
                tmp = df.iloc[i].to_dict()
                tmp = pd.DataFrame.from_dict(tmp, orient="index").T
                if result.empty:
                    result = tmp
                else:
                    result = pd.concat([result, tmp])
        ans = result
    elif search_conditions[1] == "=":   #may be string
        if df.dtypes[search_column_name] == "int64":
            search_value = int(search_value)
        ans = df[df[search_column_name] == search_value]
    elif search_conditions[1] == ">=":
        tmp = []
        result = pd.DataFrame()
        for i in range(len(df[search_column_name])):
            val = df[search_column_name][i]
            if val >= int(search_value):
                tmp = df.iloc[i].to_dict()
                tmp = pd.DataFrame.from_dict(tmp, orient="index").T
                if result.empty:
                    result = tmp
                else:
                    result = pd.concat([result, tmp])
        ans = result
    elif search_conditions[1] == "<=":
        tmp = []
        result = pd.DataFrame()
        for i in range(len(df[search_column_name])):
            val = df[search_column_name][i]
            if val <= int(search_value):
                tmp = df.iloc[i].to_dict()
                tmp = pd.DataFrame.from_dict(tmp, orient="index").T
                if result.empty:
                    result = tmp
                else:
                    result = pd.concat([result, tmp])
        ans = result
    elif search_conditions[1] == "!=":   #may be string
        tmp = []
        result = pd.DataFrame()
        for i in range(len(df[search_column_name])):
            val = df[search_column_name][i]
            if df.dtypes[search_column_name] == "int64" and val != int(search_value):
                tmp = df.iloc[i].to_dict()
                tmp = pd.DataFrame.from_dict(tmp, orient="index").T
                if result.empty:
                    result = tmp
                else:
                    result = pd.concat([result, tmp])
            elif df.dtypes[search_column_name] == "object" and val != search_value[1:-1]:
                tmp = df.iloc[i].to_dict()
                tmp = pd.DataFrame.from_dict(tmp, orient="index").T
                if result.empty:
                    result = tmp
                else:
                    result = pd.concat([result, tmp])
        ans = result
    
    if ans.empty:   #no row match this condition
        return None
    else:
        return ans


def search_index_tree(tree, key):
    if tree.has_key(key) == True:
        row_number = tree.get(key)
    else:
        row_number = None   #Not Found!!!!!
    return row_number
    

def use():   #define which database to use
    pass
    
    
def print_tree():   #print iteritems()
    pass





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
