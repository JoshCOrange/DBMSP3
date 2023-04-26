import sqlparse
import string
import re
import copy


sqlCT = '''CREATE TABLE Relation_1(
    k integer,
    val integer,
    Primary Key (k)
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

sqlS8 = '''SELECT COUNT(CustomerID), Country
            FROM Customers
            GROUP BY Country
            HAVING COUNT(CustomerID) BETWEEN 0 and 5;'''
sql_d = "DELETE FROM r_1 WHERE k > 10 and k < 100"
sql_u = "UPDATE r_1 SET k = 2, val = 10 WHERE k > 10; "

#qs = [sqlCT,sqlCI,sqlDT, sqlDI] # Create and Drop table/index
#qs = [sqlS1, sqlS2, sqlS3, sqlS4, sqlS5, sqlS6,sqlS7, sqlS8]
#qs = [sql_u]
#print(readQuery(qs))



def readQuery(qs):
    #query = input("Please enter query")
    #sql = "DROP TABLE test"
    schemaDict = {}
    execution = ()
    for sql in qs:
        tiflag = 0 # 1 for table, -1 for index, 0 for other
        parsed = sqlparse.parse(sql)
        for stmt in parsed:
            tokens = [t for t in sqlparse.sql.TokenList(stmt.tokens) if t.ttype != sqlparse.tokens.Whitespace]
            # Is it a create statements ?
            #print(type(tokens))
            #temp = str(tokens[0])
            if tokens[0].match(sqlparse.tokens.DDL, 'CREATE'):
                if tokens[1].match(sqlparse.tokens.Keyword, 'TABLE'):
                    schemaDict = createParse(1,tokens)
                    execution = ('create table', schemaDict)
                if tokens[1].match(sqlparse.tokens.Keyword, 'INDEX'):
                    schemaDict = createParse(-1, tokens)
                    execution = ('create index', schemaDict)
            if tokens[0].match(sqlparse.tokens.DDL, 'DROP'): #Should we handle if exists?
                if tokens[1].match(sqlparse.tokens.Keyword, 'TABLE'):
                    schemaDict = dropParse(1,tokens)
                    execution = ('drop table', schemaDict)
                if tokens[1].match(sqlparse.tokens.Keyword, 'INDEX'):
                    schemaDict = dropParse(-1, tokens)
                    execution = ('drop index', schemaDict)
            if tokens[0].match(sqlparse.tokens.DML, 'SELECT'):
                schemaDict = selectParse(tokens, stmt)
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
        

def createParse(flag, tokens): #Design Choice, stop user from using () in naming anything
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
            # Get the table name by looking at the tokens in reverse order till you find
            # a token with None type
            #print (f"table: {get_table_name(tokens[:i])}")
            schemaDict.update({"table_name": get_table_name(tokens[:i])})

            # Now parse the columns
            txt = token.value
            columns = txt[1:txt.rfind(")")].replace("\n","").split(",")
            for column in columns:
                c = ' '.join(column.split()).split()
                c_name = c[0].replace('\"',"")
                c_type = c[1]  # For condensed type information 
                # OR 
                #c_type = " ".join(c[1:]) # For detailed type information 
                if c[0].casefold() == "Primary".casefold():
                    #print(f"Primary key: {c[2][1:-1]}")
                    schemaDict.update({"primary_key": c[2][1:-1]})
                    continue
                if c[0].casefold() == "Foreign".casefold():
                    #print(f"Foreign key: {c[2][1:-1]}")
                    foreign_keys.append(c[2][1:-1])
                    #print(f"    Referenced Table: {c[4][0:-1]}")
                    foreign_tables.append(c[4][0:-1])
                    #print(f"    Referenced Column: {c[5][1:-1]}")
                    foreign_columns.append(c[5][1:-1])
                    delete = " ".join(c[8:])
                    #print(f"    Delete Type: {delete}")
                    foreign_deletes.append(delete)
                    schemaDict.update({"foreign_keys": foreign_keys})
                    schemaDict.update({"foreign_tables": foreign_tables})
                    schemaDict.update({"foreign_columns": foreign_columns})
                    schemaDict.update({"foreign_deletes": foreign_deletes})
                    continue
                #print (f"column: {c_name}")
                column_names.append(c_name)
                #print (f"date type: {c_type}")
                column_types.append(c_type)
                #print(type(c[0]))
                schemaDict.update({"column_names": column_names})
                schemaDict.update({"column_types": column_types})
            break

        if flag == -1 and token.match(sqlparse.tokens.Keyword, 'ON'):
            #print (f"index: {tokens[i-1]}")
            column_names = []
            schemaDict.update({"index_name": tokens[i-1].value})
            tableColumns = tokens[i+1].value.split(" ")
            #tableColumns = [''.join(c for c in s if c not in string.punctuation) for s in tableColumns if s]
            #tableColumns = [s for s in tableColumns if s]
            #print (f"table: {tableColumns[0]}")
            schemaDict.update({"table_name": tableColumns[0]})
            for col in tableColumns[1:]:
                #print (f"column: {col}")
                column_names.append(col)
            schemaDict.update({"column_names": column_names})
    #print(schemaDict)
    return schemaDict
            
def dropParse(flag, tokens):
    schemaDict = {}
    if flag == 1:
        #print("Drop Table")
        #print (f"table: {tokens[-1]}")
        schemaDict.update({"table_name": tokens[-1].value})
        return schemaDict
    for i, token in enumerate(tokens):
        if token.match(sqlparse.tokens.Keyword, 'ON'):
            #print (f"index: {tokens[i-1]}")
            schemaDict.update({"index_name": tokens[i-1].value})
            #print (f"table: {tokens[i+1]}")
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
    return schemaDict #or whatever to send to execution

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
            #print(tableName)
            columnsStartIndex = tokens.index(token) + 1
            valuesStartIndex = tokens.index(token) + 2
            break
    #print(str(tokens[columnsStartIndex]))
    #print(str(tokens[valuesStartIndex]))
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

    for i, token in enumerate(tokens):
        if token.match(sqlparse.tokens.DML, 'SELECT'):
            columns = tokens[i + 1].value.split(" ")
            #columns = [''.join(c for c in s if c not in "!\"#$%&'+, -/:;<=>?@[\]^_`{|}~") for s in columns if s]
            #String.punctuation is not used because we want to preserve * in select statment
            schemaDict.update({"columns": columns})
        
        if token.match(sqlparse.tokens.Keyword, 'FROM'): #if at end of select or next token is 'WHERE'
            table_names = tokens[i+1].value.split(" ")
            #tables_names = [''.join(c for c in s if c not in string.punctuation) for s in table_names if s]
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
            clause = str(tokens[i+1])
            parsedHaving = whereParse(clause)
            schemaDict.update({"having":parsedHaving}) 
        
        if token.match(sqlparse.tokens.Keyword, 'ORDER BY'):
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


    #print(schemaDict)
    return schemaDict

