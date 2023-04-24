from parser import *

def table_1():
    sql_CT = '''CREATE TABLE Relation_1(
    key integer,
    val string
    Primary Key (actor_id)
    )'''
    sql_I = " INSERT INTO Relation_1 (key, val) VALUES (1, 1)"
    print(readQuery([sql_I]))
    



def all_table ():
    table_1()
    #table_2()
    #table_3()
    #table_4()

all_table()