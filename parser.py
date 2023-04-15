import sqlparse
import string

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
            p# IN (2,4,8) OR
            p# BETWEEN 11 AND 15'''

#qs = [sqlCT,sqlCI,sqlDT, sqlDI] # Create and Drop table/index
qs = [sqlS1, sqlS2, sqlS3, sqlS4, sqlS5]
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
                selectParse(tokens)
        print ("---"*20)



def get_table_name(tokens): #Used for create table and create index
    for token in reversed(tokens):
        if token.ttype is None:
            return token.value
    return " "

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

def updateParse():
    pass
def insertParse():
    pass
def deleteParse():
    pass
def selectParse(tokens): #SUM, AVG, MIN, MAX, COUNT, DISTINCT
    #print(tokens)
    index_f = -1 #index of 'FROM' in sql statement
    index_w = -1
    for i, token in enumerate(tokens):
        if token.match(sqlparse.tokens.DML, 'SELECT'):
            columns = tokens[i + 1].value.split(" ")
            columns = [''.join(c for c in s if c not in "!\"#$%&'()+, -/:;<=>?@[\]^_`{|}~") for s in columns if s]
            
        if token.match(sqlparse.tokens.Keyword, 'FROM'): #if at end of select or next token is 'WHERE'
            tables = tokens[i+1].value.split(" ")
            tables = [''.join(c for c in s if c not in string.punctuation) for s in tables if s]
        if token.value.startswith("WHERE"): #only one where and its also the end
            clause = tokens[i]
        #print(token)

    #tokens[i].match(sqlparse.tokens.Keyword, 'AND') or tokens[i].match(sqlparse.tokens.Keyword, 'OR')
    
    
    
    for c in columns:
        print(f"column: {c}")
    for t in tables:
        print(f"table: {t}") 
    return
readQuery(qs)
