import sqlvalidator
import sqlparse
import string
def readQuery():
    #query = input("Please enter query")
    sql1 = '''CREATE TABLE public.actor (
    actor_id integer DEFAULT nextval('public.actor_actor_id_seq'::regclass) NOT NULL,
    first_name VARCHAR(45) NOT NULL,
    last_name character varying(45) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL,
    FOREIGN KEY (timestamp) REFERENCES Weeks (time) ON DELETE CASCADE,
    Primary Key (actor_id)
    );'''
    sql2 = "CREATE INDEX ID_test ON t1 (col_1, col_2);"
    qs = [sql1,sql2]
    #sql = "DROP TABLE test"
    for sql in qs:
        parsed = sqlparse.parse(sql)
        for stmt in parsed:
            tokens = [t for t in sqlparse.sql.TokenList(stmt.tokens) if t.ttype != sqlparse.tokens.Whitespace]
            # Is it a create statements ?
            #print(type(tokens))
            #temp = str(tokens[0])
            if tokens[0].match(sqlparse.tokens.DDL, 'CREATE'):
                #print(sqlparse.tokens)
                if tokens[1].match(sqlparse.tokens.Keyword, 'TABLE'):
                    #print("Table")
                    #for token in tokens: print(token)
                    print(createParse("table",tokens))
                if tokens[1].match(sqlparse.tokens.Keyword, 'INDEX'):
                    #print("Index")
                    #for token in tokens: print(token)
                    print(createParse("index", tokens))
    dropParse(parsed)


def get_table_name(tokens): #Used for create table and create index
    for token in reversed(tokens):
        if token.ttype is None:
            return token.value
    return " "

def createParse(keyword, tokens): #Design Choice, stop user from using () in naming anything
    #for stmt in parse:
    # Get all the tokens except whitespaces
        #tokens = [t for t in sqlparse.sql.TokenList(stmt.tokens) if t.ttype != sqlparse.tokens.Whitespace]
        #is_create_stmt = False
    for i, token in enumerate(tokens):
        # Is it a create statements ?
        #if token.match(sqlparse.tokens.DDL, 'CREATE'):
        #    is_create_stmt = True
        #    continue
        # If it was a create statement and the current token starts with "("
        #if is_create_stmt and token.value.startswith("("):
        if keyword == "table" and token.value.startswith("("):
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
            print ("---"*20)
            break
        
        if keyword == "index" and token.match(sqlparse.tokens.Keyword, 'INDEX'):
             print (f"index: {tokens[i+1]}")
             continue
        if keyword == "index" and token.match(sqlparse.tokens.Keyword, 'ON'):
            tableColumns =tokens[i+1].value.split(" ")
            tableColumns = [''.join(c for c in s if c not in string.punctuation) for s in tableColumns]
            tableColumns = [s for s in tableColumns if s]
            print (f"table: {tableColumns[0]}")
            for col in tableColumns[1:]:
                print (f"column: {col}")
            
def dropParse(parse):
    for stmt in parse:
    # Get all the tokens except whitespaces
        tokens = [t for t in sqlparse.sql.TokenList(stmt.tokens) if t.ttype != sqlparse.tokens.Whitespace]
        is_drop_stmt = False
        for i, token in enumerate(tokens):
            # Is it a create statements ?
            if token.match(sqlparse.tokens.DDL, 'DROP'):
                is_drop_stmt = True
                continue
            if is_drop_stmt and token.value == "TABLE":
                #print (f"table: {get_table_name(tokens[:i])}")
                print(tokens[i+1])
def updateParse():
    pass
def insertParse():
    pass
def deleteParse():
    pass
def selectParse():
    pass

readQuery()