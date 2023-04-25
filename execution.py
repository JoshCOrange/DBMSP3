from database import *
from parser import *
from cannedTables import *

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
        query = input("SQL> ")
        if query == "exit":
            break
        queryList = []
        queryList.append(query)
        myTuple = readQuery(queryList)
        keyword = myTuple[0]
        schemaDict = myTuple[1]

        #where does search go here
        if keyword == "create table":
            newTree = create_table(schemaDict)
            tableName = schemaDict.get('table_name')
            tableTreeRelation[table_name] = newTree
        elif keyword == "drop table":
            table_name = schemaDict.get('table_name')
            treePtr = tableTreeRelation[table_name]
            drop_table(schemaDict, treePtr)
        elif keyword == "select": #where
            selectKeyword(schemaDict)
        elif keyword == "update": #where
            updateKeyword(schemaDict)
        elif keyword == "insert":
            table_name = schemaDict['table']
            treePtr = tableTreeRelation[table_name]
            insert_table(schemaDict, treePtr)
        elif keyword == "delete": #where
            tableName = schemaDict.get('table')
            treePtr = tableTreeRelation[tableName]
            deleteKeyword(schemaDict)

def selectKeyword(schemaDict):
    conditions =  schemaDict['where']['conditions']
    table_name = schemaDict.get('table')[0]
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
    rows_one_condition = pd.DataFrame()
    all_rows = pd.DataFrame()
    #search_table(Dict, tree)
    for i in treePtr.keys():
        print(type(i))
        break
    for condition in conditions:
        whereDict['where'].update({'condition': [condition]})
        #print(whereDict['where']['condition'])
        print (search_table(whereDict, treePtr))

    #print("hello")
def parseAggregation(column): #Mainly to parse out the column names for select that's within aggrigation.  
                                #Return the column name and aggregation type
    aggregation = None
    column_name = None
    for i in range(len(column)):
        if column[i] == "(":
            start = i+1
            column_name = column[start:-1]
            aggregation = column[:i]
    print(column_name)
    print(aggregation)
    
    if aggregation is None: return column, None
    return column_name, aggregation


def updateKeyword(schemaDict):
    print("hello")
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
    start = time.time()
    #print(1)
    newTree, tableName = table_1()
    tableTreeRelation[tableName] = newTree
    print("1 done")
    end1 = time.time()
    print(end1 - start)
    '''
    newTree, tableName = table_2()
    tableTreeRelation[tableName] = newTree
    print("2 done")
    end2 = time.time()
    print(end2 - end1)
    
    newTree, tableName = table_3()
    tableTreeRelation[tableName] = newTree
    print("3 done")
    end3 = time.time()
    print(end3- end2)
    
    newTree, tableName = table_4()
    tableTreeRelation[tableName] = newTree
    print("4 done")
    end4 = time.time()
    print(end4 - end3)
    
    newTree, tableName = table_5()
    tableTreeRelation[tableName] = newTree
    print("5 done")
    end5 = time.time()
    print(end5 - end4)
    
    newTree, tableName = table_6()
    tableTreeRelation[tableName] = newTree
    print("6 done")
    end6 = time.time()
    print(end6 - end5)'''




if __name__ == '__main__':
    pd.set_option("display.max_colwidth", 10000)
    create_internal_table()
    all_table()
    #for i in tableTreeRelation.items():
    #    print(i)
    main()