from database import *
from parser import *
from cannedTables import *
import os 

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
                new_df= df.drop(table_name)
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
                ans = selectAggr(col, ans, aggr)
            if schemaDict.get('order_by') is not None:
                print(here)
                ans = orderBy(schemaDict, ans)
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


def orderBy(schemaDict, ans):
    print("order_by")
    column_order = schemaDict['oreder_by']
    for k, v in column_order.items():
        
        if v == "DESC":
            ans.sort_values(by=[k], ascending=False)
        else:
            ans.sort_values(by=[k])
    return ans



def selectAggr(col_name, ans, aggr):
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
    return result

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
        'column_name': column_names, #TODO: Need to deal with aggrigation
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
    if len(all_rows) == 0:
        ans = None
    elif column_names[0] == "*":
        ans = all_rows[0]
    else:
        ans = all_rows[0].loc[:,column_names]
    
    return ans, col, aggr

    #print("hello")


def updateKeyword(schemaDict):
    #print("hello")
    update_tree(TREE, schemaDict, KEY)

def deleteKeyword(schemaDict, treePtr):
    where = False
    if schemaDict.get('where'):
        where = True
        parsedWhere = schemaDict.get('where')

    if where == False:
        drop_table(schemaDict, treePtr)
    


#TODO
#Catch on misspelling on database accessing
#Join/Query optimization
#AND and OR


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