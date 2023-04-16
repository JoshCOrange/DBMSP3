import sqlparse
import string
import re

sqlCT = '''CREATE TABLE public.actor (
    actor_id integer DEFAULT nextval('public.actor_actor_id_seq'::regclass) NOT NULL,
    first_name VARCHAR(45) NOT NULL,
    last_name character varying(45) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL,
    FOREIGN KEY (last_update) REFERENCES Weeks (time) ON DELETE CASCADE,
    FOREIGN KEY (first_name) REFERENCES Names (first_name) ON DELETE SET NULL,
    Primary Key (actor_id)
    );'''
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
            p# BETWEEN 20 AND 25 OR
            p# IN (2,4,8)'''


sqlS6 = '''SELECT col_1, col_2, col_3, col_4 FROM table_1
            WHERE col_1 < 10
            ORDER BY col_2'''

#qs = [sqlCT,sqlCI,sqlDT, sqlDI] # Create and Drop table/index
#qs = [sqlS1, sqlS2, sqlS3, sqlS4, sqlS5]
qs = [sqlS6]
def readQuery(qs):
    #query = input("Please enter query")
    #sql = "DROP TABLE test"
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
                    createParse(1,tokens)
                if tokens[1].match(sqlparse.tokens.Keyword, 'INDEX'):
                    createParse(-1, tokens)
            if tokens[0].match(sqlparse.tokens.DDL, 'DROP'): #Should we handle if exists?
                if tokens[1].match(sqlparse.tokens.Keyword, 'TABLE'):
                    dropParse(1,tokens)
                if tokens[1].match(sqlparse.tokens.Keyword, 'INDEX'):
                    dropParse(-1, tokens)
            if tokens[0].match(sqlparse.tokens.DML, 'SELECT'):
                selectParse(tokens, stmt)
        print ("---"*20)



def get_table_name(tokens): #Used for create table and create index
    for token in reversed(tokens):
        if token.ttype is None:
            return token.value
    return " "

def whereParse(clause):
        #test = sqlparse.sql.Where(tokens)
        #help(sqlparse.sql.Where)
        #print(test.Where)
        conditions = []
        conjunctions = []
        betweenParse = re.split("BETWEEN", clause, re.IGNORECASE)
        for i in range(len(betweenParse)):
            betweenParse[i] = betweenParse[i].strip()
        #print(betweenParse)
        if len(betweenParse) > 1:
            for i, parsed in enumerate(betweenParse):
                if i == 0:
                    condis = re.split("AND|OR", parsed, re.IGNORECASE)
                    conjunc = re.findall("AND|OR", parsed, re.IGNORECASE)
                    conditions.extend(condis[:-1])
                    conjunctions.extend(conjunc)
                    continue
            
                
                if i != (len(betweenParse)):
                    temp = condis[-1].strip()
                condis = re.split("AND|OR", parsed, re.IGNORECASE)
                conjunc = re.findall("AND|OR", parsed, re.IGNORECASE)
                conditions.append(temp + " BETWEEN " + condis[0] + conjunc[0] + condis[1])
                conditions.extend(condis[2:])
                conjunctions.extend(conjunc[1:])

        else:
            conditions = re.split("AND|OR", clause, re.IGNORECASE)
            conjunctions = re.findall("AND|OR", clause, re.IGNORECASE)
        for i in range(len(conditions)):
            conditions[i] = conditions[i].strip()
        whereSchemaDict = {"conditions": conditions, "conjunctions": conjunctions}
        return whereSchemaDict
        
        
        '''
        print(condis)
        print(conjunc)'''

def createParse(flag, tokens): #Design Choice, stop user from using () in naming anything
    for i, token in enumerate(tokens):
        if flag == 1 and token.value.startswith("("):
            # Get the table name by looking at the tokens in reverse order till you find
            # a token with None type
            print (f"table: {get_table_name(tokens[:i])}")

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
                    print(f"Primary key: {c[2][1:-1]}")
                    continue
                if c[0].casefold() == "Foreign".casefold():
                    print(f"Foreign key: {c[2][1:-1]}")
                    print(f"    Referenced Table: {c[4][0:-1]}")
                    print(f"    Referenced Row: {c[5][1:-1]}")
                    delete = " ".join(c[8:])
                    print(f"    Delete Type: {delete}")
                    continue
                print (f"column: {c_name}")
                print (f"date type: {c_type}")
                #print(type(c[0]))
            break

        if flag == -1 and token.match(sqlparse.tokens.Keyword, 'ON'):
            print (f"index: {tokens[i-1]}")
            tableColumns = tokens[i+1].value.split(" ")
            tableColumns = [''.join(c for c in s if c not in string.punctuation) for s in tableColumns if s]
            #tableColumns = [s for s in tableColumns if s]
            print (f"table: {tableColumns[0]}")
            for col in tableColumns[1:]:
                print (f"column: {col}")
            
def dropParse(flag, tokens):
    if flag == 1:
        #print("Drop Table")
        print (f"table: {tokens[-1]}")
        print ("---"*20)
        return
    for i, token in enumerate(tokens):
        if token.match(sqlparse.tokens.Keyword, 'ON'):
            print (f"index: {tokens[i-1]}")
            print (f"table: {tokens[i+1]}")
            return 

def updateParse(flag, tokens): #assumes values are encolsed in single quotes
    tableName = tokens[2].value
    setClause = None
    for token in tokens:
        if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'SET':
            setClause = token
            break
    if setClause is None:
        #ERROR
        print("error")

    columns = []
    values = []

    for token in setClause.tokens[2:]:
        if token.ttype is sqlparse.Whitespace:
            continue
        elif token.ttype is sqlparse.tokens.Punctuation and token.value == ',':
            continue
        elif token.ttype is sqlparse.tokens.Name:
            columns.append(token.value)
        elif token.ttype is sqlparse.tokens.String:
            values.append(token.value.strip("'"))
        else: 
            print("error") 
        schemaDict = {'table': tableName, 'columns': columns, 'values': values}
        return schemaDict #or whatever to send to execution

def insertParse(flag, tokens): #INSERT INTO table_name (column1, column2, column3) VALUES (value1, value2, value3);
    tableName = None
    columns = []
    values = []

    for token in tokens:
        if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'INTO':
            tableName = str(tokens[tokens.index(token) + 1])
            break

    for token in tokens:
        if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'VALUES':
            valuesStartIndex = tokens.index(token) + 1
            break

        if token.ttype is sqlparse.tokens.Punctuation and token.value == '(':
            columnsStartIndex = tokens.index(token) + 1
            while True:
                columnName = str(tokens[columnsStartIndex])
                if columnName == ')':
                    break
                if columnName != ',':
                    columns.append(columnName)
                columnsStartIndex += 1

    for token in tokens[valuesStartIndex:]:
        if token.ttype is sqlparse.tokens.String:
            values.append(token.value.strip("'"))

    schemaDict = {'table': tableName, 'columns': columns, 'values': values}
    return schemaDict

def deleteParse(flag, tokens): #assumes flag is 1 if no WHERE clause exists
    tableName = None
    whereClause = None
    
    for token in tokens:
        if token.is_keyword() and token.value.upper() == 'DELETE':
            continue 
        elif token.is_keyword() and token.value.upper() == 'FROM':
            tableName = str(tokens[tokens.index(token) + 1])
        elif token.is_keyword() and token.value.upper() == 'WHERE':
            whereClause = whereParse(tokens[tokens.index(token):])
            break 
    
    if flag == 1:
        schemaDict = {'table': tableName}
        return schemaDict
    else:
        schemaDict = {'table': tableName, 'where': whereClause}
        return schemaDict


def selectParse(tokens, stmt): #SUM, AVG, MIN, MAX, COUNT, DISTINCT
    #print(tokens)
    index_f = -1 #index of 'FROM' in sql statement
    index_w = -1
    clause = ""
    schemaDict = {}
    for i, token in enumerate(tokens):
        if token.match(sqlparse.tokens.DML, 'SELECT'):
            columns = tokens[i + 1].value.split(" ")
            columns = [''.join(c for c in s if c not in "!\"#$%&'()+, -/:;<=>?@[\]^_`{|}~") for s in columns if s]
            #String.punctuation is not used because we want to preserve * in select statment
            
        if token.match(sqlparse.tokens.Keyword, 'FROM'): #if at end of select or next token is 'WHERE'
            tables = tokens[i+1].value.split(" ")
            tables = [''.join(c for c in s if c not in string.punctuation) for s in tables if s]
        if token.value.startswith("WHERE"): #only one where and its also the end
            clause = str(tokens[i])
            #print()
        if token.match(sqlparse.tokens.Keyword, 'ORDER BY'):
            columns = tokens[i + 1].value.split(" ")
    
        #print(token)

    #tokens[i].match(sqlparse.tokens.Keyword, 'AND') or tokens[i].match(sqlparse.tokens.Keyword, 'OR')
    schemaDict.update({"columns": columns})
    schemaDict.update({"table": tables}) 
    if clause:
        parsedWhere = whereParse(clause)
        schemaDict.update(parsedWhere) 
    print(schemaDict)
    return




readQuery(qs)
