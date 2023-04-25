from database import *
from parser import *
from cannedTables import *

tableTreeRelation = {}

def main():
    #list of tuples [(tableName, tree), (tableName, tree), ... ]
    
    #make dictionary hard coded in create_internal_table function
    #create all the required relations via parser
    query = "hi"
    while query != "exit":
        query = input("SQL> ")
        queryList = []
        queryList.append[query]
        myTuple = readQuery(queryList)
        keyword = myTuple[0]
        schemaDict = myTuple[1]

        #where does search go here
        if keyword == "create table":
            newTree = create_table(schemaDict)
            tableName = schemaDict.get('table_name')
            tableTreeRelation[tableName] = newTree
        elif keyword == "drop table":
            tableName = schemaDict.get('table_name')
            treePtr = tableTreeRelation[tableName]
            drop_table(schemaDict, treePtr)
        elif keyword == "select": #where
            selectKeyword(dischemaDictct)
        elif keyword == "update": #where
            updateKeyword(schemaDict)
        elif keyword == "insert":
            insert_table(schemaDict, TREE)
        elif keyword == "delete": #where
            tableName = schemaDict.get('table_name')
            treePtr = tableTreeRelation[tableName]
            deleteKeyword(schemaDict)

def selectKeyword(schemaDict):
    print("hello")

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
    
    newTree, table = table_2()
    tableTreeRelation[tableName] = newTree
    print("2 done")
    end2 = time.time()
    print(end2 - end1)
    
    newTree, table = table_3()
    tableTreeRelation[tableName] = newTree
    print("3 done")
    end3 = time.time()
    print(end3- end2)
    exit()
    
    newTree, table = table_4()
    tableTreeRelation[tableName] = newTree
    print("4 done")
    end4 = time.time()
    print(end4 - end3)
    
    newTree, table = table_5()
    tableTreeRelation[tableName] = newTree
    print("5 done")
    end5 = time.time()
    print(end5 - end4)
    
    newTree, table = table_6()
    tableTreeRelation[tableName] = newTree
    print("6 done")
    end6 = time.time()
    print(end6 - end5)




if __name__ == '__main__':
    pd.set_option("display.max_colwidth", 10000)
    create_internal_table()
    all_table()
    main()