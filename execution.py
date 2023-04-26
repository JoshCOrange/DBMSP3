from database import *
from parser import *
from cannedTables import *
import os 
import traceback
tableTreeRelation = {}

test_CT = "CREATE TABLE Relation(k integer, val integer,val_2 string Primary Key (k))"
test_Insert = "INSERT INTO Relation(k, val, val_2) VALUES (1, 2, SSS)"
test_DropT = "Drop Table Relation"

def main():
    #list of tuples [(tableName, tree), (tableName, tree), ... ]
    
    #make dictionary hard coded in create_internal_table function
    #create all the required relations via parser
    query = "hi"
    
    while query != "exit":
        try:
            query = input("SQL> ").strip()
            if query == "exit":
                break
            if query == "":
                continue
            queryList = []
            queryList.append(query)
            myTuple = readQuery(queryList)
            keyword = myTuple[0]
            schemaDict = myTuple[1]

            #where does search go here
            if keyword == "create table":
                newTree = create_table(schemaDict)
                table_name = schemaDict.get('table_name')
                tableTreeRelation[table_name] = newTree
            
            elif keyword == "drop table":
                table_name = schemaDict.get('table_name')
                treePtr = tableTreeRelation[table_name]
                drop_table(schemaDict, treePtr)
                os.remove(table_name + ".csv")
                df = pd.read_csv("internal_table.csv")
                # Set the index of the DataFrame to the country name
                with open("internal_table.csv", 'r', newline='') as f: 
                    df = df.set_index("table_name")
                    new_df = df.drop(table_name)
                    new_df = new_df.reset_index()
                    new_df.to_csv("internal_table.csv", index=False)

            elif keyword == "select": #where
                if len(schemaDict['columns']) == 1:
                    col, aggr = parseAggregation(schemaDict['columns'][0])
                if schemaDict.get('where') is not None:
                    ans, col, aggr = selectKeyword(schemaDict)
                else:
                    ans = pd.read_csv(schemaDict.get('table_name')[0] + ".csv")
                if schemaDict.get('group_by') is not None:
                    ans = groupBy(schemaDict, ans)
                if aggr is not None:
                    ans = selectAggr(col, ans, aggr, schemaDict)
                    if schemaDict.get('having') is not None:
                        ans = having(schemaDict, ans, col)
                if schemaDict.get('order_by') is not None:
                    ans = orderBy(schemaDict, ans)
                if schemaDict.get('join') is not None:
                    ans = joinKeyword(schemaDict, ans)
                print(ans)
            
            elif keyword == "update": #where
                updateKeyword(schemaDict)
            
            elif keyword == "insert":
                table_name = schemaDict['table_name']
                treePtr = tableTreeRelation[table_name]
                insert_table(schemaDict, treePtr)
            
            elif keyword == "delete": #where
                tableName = schemaDict.get('table_name')
                treePtr = tableTreeRelation[tableName]
                deleteKeyword(schemaDict)
        except Exception as e:
            print("There is probably a syntax error in SQL query")
            print(e)
            print(traceback.format_exc())
            continue

def joinKeyword(schemaDict, ans):
    #table_one = schemaDict["tableOne"]
    table_two = schemaDict['join']["tableTwo"]
    #tableOneFile = table_one + ".csv"
    tableTwoFile = table_two + ".csv"
    #dfOne = pd.read_csv(tableOneFile)
    dfOne = ans
    dfTwo = pd.read_csv(tableTwoFile)
    

    headings = list(dfOne.columns)
    headings.extend(list(dfTwo.columns))

    dfMerged = pd.DataFrame(columns=headings)

    difference = abs(len(dfOne) - len(dfTwo))
    locNum = 0
    if difference > 500:
        #Nested
        for index, row in dfOne.iterrows():
            valOne = row[schemaDict['join']["columnOne"]]
            for indexTwo, rowTwo in dfTwo.iterrows():
                valTwo = rowTwo[schemaDict['join']["columnTwo"]]
                if schemaDict['join']["operator"] == "=":
                    if valOne == valTwo:
                        newRow = list(row)
                        newRow.extend(list(rowTwo))
                        dfMerged.loc[locNum] = newRow
                        locNum += 1
                if schemaDict['join']["operator"] == ">":
                    if valOne > valTwo:
                        newRow = list(row)
                        newRow.extend(list(rowTwo))
                        dfMerged.loc[locNum] = newRow
                        locNum += 1
                if schemaDict['join']["operator"] == "<":
                    if valOne < valTwo:
                        newRow = list(row)
                        newRow.extend(list(rowTwo))
                        dfMerged.loc[locNum] = newRow
                        locNum += 1
                if schemaDict['join']["operator"] == ">=":
                    if valOne >= valTwo:
                        newRow = list(row)
                        newRow.extend(list(rowTwo))
                        dfMerged.loc[locNum] = newRow
                        locNum += 1
                if schemaDict['join']["operator"] == "<=":
                    if valOne <= valTwo:
                        newRow = list(row)
                        newRow.extend(list(rowTwo))
                        dfMerged.loc[locNum] = newRow
                        locNum += 1
                if schemaDict['join']["operator"] == "!=":
                    if valOne != valTwo:
                        newRow = list(row)
                        newRow.extend(list(rowTwo))
                        dfMerged.loc[locNum] = newRow
                        locNum += 1
    else:
        #merge
        i = 0
        j = 0
            
        while i < len(dfOne) and j < len(dfTwo):
            valOne = dfOne.loc[i, schemaDict['join']["columnOne"]]
            valTwo = dfTwo.loc[j, schemaDict['join']["columnTwo"]]
            if schemaDict['join']["operator"] == "=":
                if valOne == valTwo:
                    newRow = list(dfOne.iloc[i])
                    newRow.extend(list(dfTwo.iloc[j]))
                    dfMerged.loc[locNum] = newRow
                    locNum += 1
                    j += 1
                else:
                    i += 1
            if schemaDict['join']["operator"] == ">":
                if valOne > valTwo:
                    newRow = list(dfOne.iloc[i])
                    newRow.extend(list(dfTwo.iloc[j]))
                    dfMerged.loc[locNum] = newRow
                    locNum += 1
                    j += 1
                else:
                    i += 1
            if schemaDict['join']["operator"] == "<":
                if valOne < valTwo:
                    newRow = list(dfOne.iloc[i])
                    newRow.extend(list(dfTwo.iloc[j]))
                    dfMerged.loc[locNum] = newRow
                    locNum += 1
                    j += 1
                else:
                    i += 1
            if schemaDict['join']["operator"] == "!=":
                if valOne != valTwo:
                    newRow = list(dfOne.iloc[i])
                    newRow.extend(list(dfTwo.iloc[j]))
                    dfMerged.loc[locNum] = newRow
                    locNum += 1
                    j += 1
                else:
                    i += 1
            if schemaDict['join']["operator"] == "<=":
                if valOne <= valTwo:
                    newRow = list(dfOne.iloc[i])
                    newRow.extend(list(dfTwo.iloc[j]))
                    dfMerged.loc[locNum] = newRow
                    locNum += 1
                    j += 1
                else:
                    i += 1
            if schemaDict['join']["operator"] == ">=":
                if valOne >= valTwo:
                    newRow = list(dfOne.iloc[i])
                    newRow.extend(list(dfTwo.iloc[j]))
                    dfMerged.loc[locNum] = newRow
                    locNum += 1
                    j += 1
                else:
                    i += 1
            if (j == len(dfTwo)):
                j = 0
                i += 1

    #print(dfMerged)
    return dfMerged

def having(schemaDict, ans, col):
    #print(ans)
    conditions =  schemaDict['having']['conditions']
    conjunctions = schemaDict['having']['conjunctions']
    column_names = schemaDict['columns']
    havingDict = {
        "df" : ans,
        'column_name': schemaDict['columns'],
        'having': {}
    }
    all_rows = []
    for condition in conditions:
        havingDict['having'].update({'condition': [condition]})
        #print(whereDict['where']['condition'])
        all_rows.append(search_table_having(havingDict))
    
    #print(all_rows)
    for i, conjunction in enumerate(conjunctions):
        if len(all_rows) == 1:
            if conjunction.upper() == "AND":
                all_rows.pop(0)
            break
        if conjunction.upper() == "OR":
            if all_rows[0] is None:
                all_rows.pop(0)
                continue
            if all_rows[1] is None:
                all_rows.pop(1)
                continue
            new_rows = orConjunctions (all_rows[0],all_rows[1])
        if conjunction.upper() == "AND":
            if all_rows[0] is None or all_rows[1] is None:
                all_rows.pop(0)
                all_rows.pop(0)
                continue
            new_rows = andConjunctions (all_rows[0],all_rows[1])
        all_rows.pop(0)
        all_rows.pop(0)
        all_rows.insert(0, new_rows)
    #ans.loc[:,return_columns]
    #ans = all_rows[0]
    if len(all_rows) == 0 or all_rows[0] is None:
        ans = None
    elif column_names[0] == "*":
        ans = all_rows[0]
    else:
        ans = all_rows[0].loc[:,column_names]
    
    return ans
    
    


def orderBy(schemaDict, ans):
    column_order = schemaDict['order_by']
    columns = column_order['col_orders']
    orders = column_order['orders']
    for i in range(len(orders)):
        if orders[i] == "DESC":
            ans = ans.sort_values(by=[columns[i]], ascending=False)
        else:
            ans = ans.sort_values(by=[columns[i]])
    return ans


def selectAggr(col_name, ans, aggr, schemaDict):
    result = None
    if aggr.upper() == "MIN":
        result = min(ans[col_name].to_list())
    if aggr.upper() == "MAX":
        result = max(ans[col_name].to_list())
    if aggr.upper() == "AVG":
        result = sum(ans[col_name].to_list()) / len(ans[col_name].to_list())
    if aggr.upper() == "SUM":
        result = sum(ans[col_name].to_list())
    if aggr.upper() == "COUNT":
        result = len(ans[col_name].to_list())
    new_df = pd.DataFrame([result], columns=schemaDict['columns'])
    return new_df

def groupBy(schemaDict, df):
    group_col = schemaDict["group_by"]
    df.groupby([group_col[0]])
    return df
    

#ans.loc[:,return_columns]
def orConjunctions (df_1, df_2):
    new_df = pd.concat([df_1,df_2]).drop_duplicates().reset_index(drop=True)
    return new_df

def andConjunctions (df_1, df_2):
    new_df = pd.merge(df_1, df_2).reset_index(drop=True)
    return new_df

def parseAggregation(column): #Mainly to parse out the column names for select that's within aggrigation.  
                                #Return the column name and aggregation type
    aggregation = None
    column_name = None
    for i in range(len(column)):
        if column[i] == "(":
            start = i+1
            column_name = column[start:-1]
            aggregation = column[:i]
    #print(column_name)
    #print(aggregation)
    
    if aggregation is None: 
        return column, None
    return column_name, aggregation



def selectKeyword(schemaDict):
    #if schemaDict.get('where') is None:
    conditions =  schemaDict['where']['conditions']
    conjunctions = schemaDict['where']['conjunctions']
    table_name = schemaDict.get('table_name')[0]
    treePtr = tableTreeRelation[table_name]
    #Dict = {
    # 'table_name': table_name
    # 'column_name': column_1, column_2, .... essentially the columns needed for select
    # 'where': {condition: condition statement}
    #}
    column_names = []
    if len(schemaDict['columns']) == 1:
        col, aggr = parseAggregation(schemaDict['columns'][0]) #aggr isn't tracked for now
        column_names.append(col)
    else:
        column_names = schemaDict['columns']

    whereDict = {"table_name": table_name,
        'column_name': column_names, 
        'where': {'condition': ""}
        }
    all_rows = []
    #search_table(Dict, tree)

    for condition in conditions:
        whereDict['where'].update({'condition': [condition]})
        #print(whereDict['where']['condition'])
        all_rows.append(search_table(whereDict, treePtr))
    #print(all_rows)
    for i, conjunction in enumerate(conjunctions):
        if len(all_rows) == 1:
            if conjunction.upper() == "AND":
                all_rows.pop(0)
            break
        if conjunction.upper() == "OR":
            if all_rows[0] is None:
                all_rows.pop(0)
                continue
            if all_rows[1] is None:
                all_rows.pop(1)
                continue
            new_rows = orConjunctions (all_rows[0],all_rows[1])
        if conjunction.upper() == "AND":
            if all_rows[0] is None or all_rows[1] is None:
                all_rows.pop(0)
                all_rows.pop(0)
                continue
            new_rows = andConjunctions (all_rows[0],all_rows[1])
        all_rows.pop(0)
        all_rows.pop(0)
        all_rows.insert(0, new_rows)
    #ans.loc[:,return_columns]
    #ans = all_rows[0]
    if len(all_rows) == 0 or all_rows[0] is None:
        ans = None
    elif column_names[0] == "*":
        ans = all_rows[0]
    else:
        ans = all_rows[0].loc[:,column_names]
    
    return ans, col, aggr

    #print("hello")


def updateKeyword(schemaDict):
    conditions =  schemaDict['where']['conditions']
    conjunctions = schemaDict['where']['conjunctions']
    table_name = schemaDict.get('table_name')
    treePtr = tableTreeRelation[table_name]
    column_names = "*"

    whereDict = {"table_name": table_name,
        'column_name': column_names,
        'where': {'condition': ""}
        }
    all_rows = []
    
    for condition in conditions:
        whereDict['where'].update({'condition': [condition]})
        #print(whereDict['where']['condition'])
        all_rows.append(search_table(whereDict, treePtr))
    #print(all_rows)
    for i, conjunction in enumerate(conjunctions):
        if len(all_rows) == 1:
            if conjunction.upper() == "AND":
                all_rows.pop(0)
            break
        if conjunction.upper() == "OR":
            if all_rows[0] is None:
                all_rows.pop(0)
                continue
            if all_rows[1] is None:
                all_rows.pop(1)
                continue
            new_rows = orConjunctions (all_rows[0],all_rows[1])
        if conjunction.upper() == "AND":
            if all_rows[0] is None or all_rows[1] is None:
                all_rows.pop(0)
                all_rows.pop(0)
                continue
            new_rows = andConjunctions (all_rows[0],all_rows[1])
        all_rows.pop(0)
        all_rows.pop(0)
        all_rows.insert(0, new_rows)
    #print(all_rows)
    df = pd.read_csv("internal_table.csv")
    # Set the index of the DataFrame to the country name
    specific_row = df[df['table_name'] == schemaDict['table_name']]
    schema = readDict(specific_row.loc[:,"schemaDict"].to_string(index=False))
    primary_key_column_name = schema['primary_key']   #since we only have one primary key
    orig_primary_keys = all_rows[0].loc[:,primary_key_column_name].to_list()
    if type(orig_primary_keys[0]) is int:
        index = schemaDict['columns'].index(primary_key_column_name[0])
        schemaDict['values'][index] = int(schemaDict['values'][index])
    for orig_key in orig_primary_keys:
        update_table(treePtr, schemaDict, orig_key)
    

    

    #update_tree(TREE, schemaDict, KEY)

def deleteKeyword(schemaDict):
    where = False
    if schemaDict.get('where'):
        where = True
        parsedWhere = schemaDict.get('where')
    if where == False:
        drop_table(schemaDict, treePtr)
    
    conditions =  schemaDict['where']['conditions']
    conjunctions = schemaDict['where']['conjunctions']
    table_name = schemaDict.get('table_name')
    treePtr = tableTreeRelation[table_name]
    column_names = "*"

    whereDict = {"table_name": table_name,
        'column_name': column_names,
        'where': {'condition': ""}
        }
    all_rows = []
    
    for condition in conditions:
        whereDict['where'].update({'condition': [condition]})
        #print(whereDict['where']['condition'])
        all_rows.append(search_table(whereDict, treePtr))
    #print(all_rows)
    for i, conjunction in enumerate(conjunctions):
        if len(all_rows) == 1:
            if conjunction.upper() == "AND":
                all_rows.pop(0)
            break
        if conjunction.upper() == "OR":
            if all_rows[0] is None:
                all_rows.pop(0)
                continue
            if all_rows[1] is None:
                all_rows.pop(1)
                continue
            new_rows = orConjunctions (all_rows[0],all_rows[1])
        if conjunction.upper() == "AND":
            if all_rows[0] is None or all_rows[1] is None:
                all_rows.pop(0)
                all_rows.pop(0)
                continue
            new_rows = andConjunctions (all_rows[0],all_rows[1])
        all_rows.pop(0)
        all_rows.pop(0)
        all_rows.insert(0, new_rows)
    #print(all_rows)
    df = pd.read_csv("internal_table.csv")
    # Set the index of the DataFrame to the country name
    specific_row = df[df['table_name'] == schemaDict['table_name']]
    schema = readDict(specific_row.loc[:,"schemaDict"].to_string(index=False))
    primary_key_column_name = schema['primary_key']   #since we only have one primary key
    primary_keys = all_rows[0].loc[:,primary_key_column_name].to_list()


    
    
    for key in primary_keys:
        df = pd.read_csv(f"{schemaDict['table_name']}.csv")
        # Set the index of the DataFrame to the country name
        with open(f"{schemaDict['table_name']}.csv", 'r', newline='') as f: 
            df = df.set_index(primary_key_column_name)
            new_df = df.drop(key)
            new_df = new_df.reset_index()
            new_df.to_csv(f"{schemaDict['table_name']}.csv", index=False)
        #print(key)
        delete_index_tree(treePtr, key, table_name)
    '''for key in treePtr.keys():
        print(key)
        '''
    

def thetaJoin(df_1, df_2, col_1, col_2, condition):
    list1 = df_1.loc[:,[col_1]].to_list()
    list2 = df_2.loc[:,[col_2]].to_list()
    if len(list1) > len(list2):
        outerList  =list1
        innerList = list2
    else:
        outerList  =list2
        innerList = list1
    
    for val in outerList:
        pass



def all_table ():
    newTree, tableName = table_0()
    tableTreeRelation[tableName] = newTree
    print("0 done")
    start = time.time()
    #print(1)
    newTree, tableName = table_1()
    tableTreeRelation[tableName] = newTree
    print("1 done")
    end1 = time.time()
    print("Time: ", end1 - start)
    '''
    newTree, tableName = table_2()
    tableTreeRelation[tableName] = newTree
    print("2 done")
    end2 = time.time()
    print("Time: ", end2 - end1)
    
    newTree, tableName = table_3()
    tableTreeRelation[tableName] = newTree
    print("3 done")
    end3 = time.time()
    print("Time: ", end3 - end2)
    
    newTree, tableName = table_4()
    tableTreeRelation[tableName] = newTree
    print("4 done")
    end4 = time.time()
    print("Time: ", end4 - end3)
    
    newTree, tableName = table_5()
    tableTreeRelation[tableName] = newTree
    print("5 done")
    end5 = time.time()
    print("Time: ", end5 - end4)
    
    newTree, tableName = table_6()
    tableTreeRelation[tableName] = newTree
    print("6 done")
    end6 = time.time()
    print("Time: ", end6 - end5)'''




if __name__ == '__main__':
    pd.set_option("display.max_colwidth", 10000)
    create_internal_table()
    all_table()
    #for i in tableTreeRelation.items():
    #    print(i)
    main()