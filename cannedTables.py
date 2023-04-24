from parser import *

def table_1():
    sql_CT = '''CREATE TABLE Relation_1(
    key integer,
    val integer
    Primary Key (key)
    )'''
    readQuery([sql_CT])
    for i in range(1000):
        sql_I = "INSERT INTO Relation_1 (key, val) VALUES (" + str(i+1) +", " + str(i+1) + ")"
        readQuery([sql_I])

def table_2():
    sql_CT = '''CREATE TABLE Relation_2(
    key integer,
    val integer
    Primary Key (key)
    )'''
    readQuery([sql_CT])
    for i in range(1000):
        sql_I = "INSERT INTO Relation_1 (key, val) VALUES (" + str(i+1) +", 1)"
        readQuery([sql_I])

def table_3():
    sql_CT = '''CREATE TABLE Relation_3(
    key integer,
    val integer
    Primary Key (key)
    )'''
    readQuery([sql_CT])
    for i in range(10000):
        sql_I = "INSERT INTO Relation_1 (key, val) VALUES (" + str(i+1) +", " + str(i+1) + ")"
        readQuery([sql_I])
    
def table_4():
    sql_CT = '''CREATE TABLE Relation_4(
    key integer,
    val integer
    Primary Key (key)
    )'''
    readQuery([sql_CT])
    for i in range(10000):
        sql_I = "INSERT INTO Relation_1 (key, val) VALUES (" + str(i+1) +", 1)"
        readQuery([sql_I])

def table_5():
    sql_CT = '''CREATE TABLE Relation_5(
    key integer,
    val integer
    Primary Key (key)
    )'''
    readQuery([sql_CT])
    for i in range(100000):
        sql_I = "INSERT INTO Relation_1 (key, val) VALUES (" + str(i+1) +", " + str(i+1) + ")"
        readQuery([sql_I])


def table_6():
    sql_CT = '''CREATE TABLE Relation_6(
    key integer,
    val integer
    Primary Key (key)
    )'''
    readQuery([sql_CT])
    for i in range(100000):
        sql_I = "INSERT INTO Relation_1 (key, val) VALUES (" + str(i+1) +", 1)"
        readQuery([sql_I])




def all_table ():
    table_1()
    table_2()
    table_3()
    table_4()
    table_5()
    table_6()


all_table()