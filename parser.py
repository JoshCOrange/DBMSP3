import sqlparse
import string
import re
import copy


sqlCT = '''CREATE TABLE DEF(Name string, ID integer, Apt# integer, FOREIGN KEY (k) REFERENCES ABC (Name) ON DELETE CASCADE, Primary Key (ID))
    )'''
sqlCI = "CREATE INDEX ID_test ON t1 (col_1, col_2);"
sqlDT = "DROP TABLE table_1"
sqlDI = "DROP INDEX index_1 ON table_1"
sqlS1 = "SELECT col_1 FROM table_1"
sqlS2 = "SELECT col_1, col_2, col_3, col_4 FROM table_1"
sqlS3 = "SELECT col_1, col_2, col_3, col_4 FROM table_1, table_2, table_3, table_4, table_5"
sqlS4 = '''SELECT * FROM PARTS
            WHERE name LIKE ‘W%’'''
sqlS5 = '''SELECT *
            FROM PARTS
            WHERE name LIKE ‘W%’ OR
            p# BETWEEN 11 AND 15 OR
            a_num >= 20 OR
            p# IN (2,4,8)'''
sqlS6 = '''SELECT col_1, col_2, col_3, col_4 FROM table_1
            WHERE col_1 < 10
            ORDER BY col_2, col_1 DESC, col_3 ASC, col_4'''

sqlS7 = '''SELECT col_1, col_2, col_3, col_4 FROM table_1
            WHERE col_1 < 10
            GROUP BY col_2, col_4
            ORDER BY col_1 DESC, col_3 ASC'''

sqlS8 = '''SELECT COUNT(CustomerID)
            FROM Customers
            HAVING COUNT(CustomerID) BETWEEN 0 and 5;'''
sqlS9 = '''SELECT COUNT(CustomerID)
            FROM Customers
            HAVING COUNT(CustomerID) BETWEEN 0 and 5
            ORDER BY COUNT(CustomerID)'''
sql_d = "DELETE FROM r_1 WHERE k > 10 and k < 100"
sql_u = "UPDATE r_1 SET k = 2, val = 10 WHERE k > 10; "
sql_j1 = "SELECT k FROM Relation_1 WHERE k < 10 JOIN Relation_2 ON k = k"
sql_j2 = "SELECT k FROM Relation_1 WHERE k < 10 JOIN Relation_2 ON k > k"
sql_j3 = "SELECT k FROM Relation_1 WHERE k < 10 JOIN Relation_2 ON k != k"
sql_j4 = "SELECT k FROM Relation_1 WHERE k < 10 JOIN Relation_2 ON k <= k"

#qs = [sqlCT,sqlCI,sqlDT, sqlDI] # Create and Drop table/index
#qs = [sqlS1, sqlS2, sqlS3, sqlS4, sqlS5, sqlS6,sqlS7, sqlS8]
qs = [sqlCT]



def readQuery(qs):

    schemaDict = {}
    execution = ()
    join = None
    for sql in qs:
        tiflag = 0 # 1 for table, -1 for index, 0 for other
        if "JOIN" in sql:
            tmp = sql.split("JOIN")
            parsed = sqlparse.parse(tmp[0])
            join = sqlparse.parse(tmp[1])
        else:
            parsed = sqlparse.parse(sql)
        for stmt in parsed:
            tokens = [t for t in sqlparse.sql.TokenList(stmt.tokens) if t.ttype != sqlparse.tokens.Whitespace]

            if tokens[0].match(sqlparse.tokens.DDL, 'CREATE'):
                if tokens[1].match(sqlparse.tokens.Keyword, 'TABLE'):
                    schemaDict = createParse(1,tokens)
                    execution = ('create table', schemaDict)
                if tokens[1].match(sqlparse.tokens.Keyword, 'INDEX'):
                    schemaDict = createParse(-1, tokens)
                    execution = ('create index', schemaDict)
            if tokens[0].match(sqlparse.tokens.DDL, 'DROP'): 
                if tokens[1].match(sqlparse.tokens.Keyword, 'TABLE'):
                    schemaDict = dropParse(1,tokens)
                    execution = ('drop table', schemaDict)
                if tokens[1].match(sqlparse.tokens.Keyword, 'INDEX'):
                    schemaDict = dropParse(-1, tokens)
                    execution = ('drop index', schemaDict)
            if tokens[0].match(sqlparse.tokens.DML, 'SELECT'):
                schemaDict = selectParse(tokens, stmt)
                if join is not None:
                    for stmt in join:
                        tokens = [t for t in sqlparse.sql.TokenList(stmt.tokens) if t.ttype != sqlparse.tokens.Whitespace]
                        thisTable = schemaDict["table_name"]
                        parsedJoin = joinParse(tokens, thisTable)
                        schemaDict.update({"join": parsedJoin})
                execution = ('select', schemaDict)
            if tokens[0].match(sqlparse.tokens.DML, 'UPDATE'):
                schemaDict = updateParse(tokens)
                execution = ('update', schemaDict)
            if tokens[0].match(sqlparse.tokens.DML, 'INSERT'):
                schemaDict = insertParse(tokens)
                execution = ('insert', schemaDict) 
            if tokens[0].match(sqlparse.tokens.DML, 'DELETE'):
                flag = 1
                for i, token in enumerate(tokens):
                    if token.value.startswith("WHERE"): flag = 0
                schemaDict = deleteParse(flag, tokens)
                execution = ('delete', schemaDict)
        #print(schemaDict)
        return execution
        #print ("---"*20)


def joinParse(tokens, tableName):
    joinSchema = {"tableOne": "", "columnOne": "", "tableTwo": "", "columnTwo": "", "operator": "" }
    tableOne = tableName
    tableTwo = tokens[0].value

    for i, token in enumerate(tokens):
        #print(token)
        if token.match(sqlparse.tokens.Keyword, 'ON'):
            tmp = tokens[i+1].value.split(" ")
            columnOne = tmp[0]
            operator = tmp[1]
            columnTwo = tmp[2]
            break
    joinSchema["tableOne"] = tableOne
    joinSchema["tableTwo"] = tableTwo
    joinSchema["columnOne"] = columnOne
    joinSchema["columnTwo"] = columnTwo
    joinSchema["operator"] = operator

    return joinSchema


def get_table_name(tokens): #Used for create table and create index
    for token in reversed(tokens):
        if token.ttype is None:
            return token.value
    return " "

def whereParse(clause): #Due to "BETWEEN # AND #" the where clause parse have to do work around it
        conditions = []
        conjunctions = []
        condis = []
        conjunc = []
        split = r"AND|aND|AnD|ANd|anD|aNd|And|and|OR|oR|Or|or"
        betweenParse = re.split("BETWEEN", clause, re.IGNORECASE)
        #print(betweenParse)
        for i in range(len(betweenParse)):
            betweenParse[i] = ' '.join(betweenParse[i].replace("\n", "").strip().split())
        #print(betweenParse)
        if len(betweenParse) > 1:
            for i, parsed in enumerate(betweenParse):
                if i > 0 and i != (len(betweenParse)):
                    temp = condis[-1].strip()
                
                #print(parsed)
                condis = re.split(split, parsed)
                #print(condis)
                conjunc = re.findall(split, parsed)
                #print(conjunc)
                
                if i == 0:
                    conditions.extend(condis[:-1])
                    conjunctions.extend(conjunc)
                    continue

                if i != (len(betweenParse)):
                    conditions.append(temp + " BETWEEN " + condis[0] + conjunc[0] + condis[1])
                    conjunctions.extend(conjunc[1:])
                    if i == (len(betweenParse) - 1):
                        conditions.extend(condis[2:])
                        break
                    conditions.extend(condis[3:])

        else:
            conditions = re.split(split, clause)
            conjunctions = re.findall(split, clause)
        
        for i in range(len(conditions)):
            if i  == 0:
                conditions[i] = " ".join(conditions[i].split(" ")[1:]).strip()
            conditions[i] = conditions[i].strip()
        
        whereSchemaDict = {"conditions": conditions, "conjunctions": conjunctions}
        return whereSchemaDict
        

def createParse(flag, tokens):
    schemaDict = {
            'table_name': "", 
            'primary_key':[],
            'column_names': [], 
            'column_types': [],
            "foreign_keys" : [],
            "foreign_tables" : [],
            "foreign_columns" : [],
            "foreign_deletes" : []
                        }
    for i, token in enumerate(tokens):
        if flag == 1 and token.value.startswith("("):
            column_names = []
            column_types = []
            foreign_keys = []
            foreign_tables = []
            foreign_columns = []
            foreign_deletes = []
            # Get the table name by looking at the tokens in reverse order till find a token with None type

            schemaDict.update({"table_name": get_table_name(tokens[:i])})

            # Now parse the columns
            txt = token.value
            columns = txt[1:txt.rfind(")")].replace("\n","").split(",")
            for column in columns:
                c = ' '.join(column.split()).split()
                c_name = c[0].replace('\"',"")
                c_type = c[1]  
                if c[0].casefold() == "Primary".casefold():
                    schemaDict.update({"primary_key": c[2][1:-1]})
                    continue
                if c[0].casefold() == "Foreign".casefold():
                    foreign_keys.append(c[2][1:-1])
                    foreign_tables.append(c[4][0:])
                    foreign_columns.append(c[5][1:-1])
                    delete = " ".join(c[8:])
                    foreign_deletes.append(delete)
                    schemaDict.update({"foreign_keys": foreign_keys})
                    schemaDict.update({"foreign_tables": foreign_tables})
                    schemaDict.update({"foreign_columns": foreign_columns})
                    schemaDict.update({"foreign_deletes": foreign_deletes})
                    continue

                column_names.append(c_name)
                column_types.append(c_type)
                schemaDict.update({"column_names": column_names})
                schemaDict.update({"column_types": column_types})
            break

        if flag == -1 and token.match(sqlparse.tokens.Keyword, 'ON'):
            #print (f"index: {tokens[i-1]}")
            column_names = []
            schemaDict.update({"index_name": tokens[i-1].value})
            tableColumns = tokens[i+1].value.split(" ")

            schemaDict.update({"table_name": tableColumns[0]})
            for col in tableColumns[1:]:

                column_names.append(col)
            schemaDict.update({"column_names": column_names})

    return schemaDict
            
def dropParse(flag, tokens):
    schemaDict = {}
    if flag == 1:

        schemaDict.update({"table_name": tokens[-1].value})
        return schemaDict
    for i, token in enumerate(tokens):
        if token.match(sqlparse.tokens.Keyword, 'ON'):

            schemaDict.update({"index_name": tokens[i-1].value})

            schemaDict.update({"table_name": tokens[i+1].value})
    return schemaDict

def updateParse(tokens): #assumes values are encolsed in single quotes
    tableName = tokens[1].value
    columns = []
    values = []
    setClause = []
    for i, token in enumerate(tokens):
        #print(token)
        if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'SET':
            setClause = tokens[i+1].value.split(',')
        elif token.value.startswith("WHERE"):
            clause = str(tokens[i])
            whereClause = whereParse(clause)

    for clause in setClause:
        tmp = clause.split('=')
        columns.append(tmp[0].strip())
        values.append(tmp[1].strip())
        #print(tmp)
    schemaDict = {'table_name': tableName, 'columns': columns, 'values': values, 'where': whereClause}
    return schemaDict 

def insertParse(tokens): #INSERT INTO table_name (column1, column2, column3) VALUES (value1, value2, value3);
    tableName = None
    columns = []
    values = []

    for i, token in enumerate(tokens):
        if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'INTO':
            tableName = str(tokens[tokens.index(token) + 1])
            for i in range(len(tableName)):
                #print(tableName[i] == '(')
                if tableName[i] == '(':
                    tableName = tableName[:i]
                    break
            columnsStartIndex = tokens.index(token) + 1
            valuesStartIndex = tokens.index(token) + 2
            break

    columns = str(tokens[columnsStartIndex]).split('(')[1:] # split on ( to find the columns
    columns  = columns[0].split(', ') # split on ', ' for each column name
    columns[-1] = columns[-1][:-1] # remove ')' on the last column name

    values = str(tokens[valuesStartIndex]).split('(')[1:] # split on ( to find the values
    values  = values[0].split(', ') # split on ', ' for each value
    values[-1] = values[-1][:-1] # remove ')' on the last value
 
    schemaDict = {'table_name': tableName, 'columns': columns, 'values': values}
    return schemaDict

def deleteParse(flag, tokens): #assumes flag is 1 if no WHERE clause exists
    tableName = None
    whereClause = None
    
    for token in tokens:
        if token.match(sqlparse.tokens.Keyword, 'DELETE'):
            continue 
        elif token.match(sqlparse.tokens.Keyword, 'FROM'):
            tableName = str(tokens[tokens.index(token) + 1])
        elif token.value.startswith("WHERE"):
            whereClause = whereParse(token.value)
            break 
    
    if flag == 1:
        schemaDict = {'table_name': tableName}
        return schemaDict
    else:
        schemaDict = {'table_name': tableName, 'where': whereClause}
        return schemaDict


def selectParse(tokens, stmt): #SUM, AVG, MIN, MAX, COUNT, DISTINCT
    #print(tokens)
    schemaDict = {}
    h_i = None

    for i, token in enumerate(tokens):
        if token.match(sqlparse.tokens.DML, 'SELECT'):
            columns = tokens[i + 1].value.split(" ")
            schemaDict.update({"columns": columns})
        
        if token.match(sqlparse.tokens.Keyword, 'FROM'): 
            table_names = tokens[i+1].value.split(" ")
            schemaDict.update({"table_name": table_names}) 
        
        if token.value.upper().startswith("WHERE"):
            clause = str(tokens[i])
            parsedWhere = whereParse(clause)
            schemaDict.update({"where": parsedWhere})
        
        if token.match(sqlparse.tokens.Keyword, 'GROUP BY'):
            group_by = []
            group_stmt = tokens[i + 1].value.split(",")
            for col in group_stmt:
                group_by.append(col.strip())
            schemaDict.update({"group_by": group_by})
        if token.match(sqlparse.tokens.Keyword, 'HAVING'):
            h_i = i
        
        if token.match(sqlparse.tokens.Keyword, 'ORDER BY'):
            if h_i is not None:
                clause = tokens[h_i:i]
                clause_list = []
                clause_str = ""
                for c in clause:
                    clause_list.append(c.value)
                clause_str = " ".join(clause_list)
                parsedHaving = whereParse(clause_str)
                schemaDict.update({"having":parsedHaving}) 
            order_stmt = tokens[i + 1].value.split(",")
            col_orders = []
            orders = []
            for col in order_stmt:
                tmp = col.strip().split(" ")

                col_orders.append(tmp[0])
                if len(tmp) > 1:
                    orders.append(tmp[1].upper())
                else:
                    orders.append("ASC")
            schemaDict.update({"order_by": {"col_orders": col_orders,"orders": orders }})

    if h_i is not None and schemaDict.get("having") is None:
            clause = tokens[h_i:]
            clause_list = []
            clause_str = ""
            for c in clause:
                clause_list.append(c.value)
            clause_str = " ".join(clause_list)
            #print(clause_str)
            parsedHaving = whereParse(clause_str)
            schemaDict.update({"having":parsedHaving}) 
    return schemaDict
print(readQuery(qs))
