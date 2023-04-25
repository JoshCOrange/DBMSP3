from parser import *
import time
from execution import *

def table_1():
    sql_CT = '''CREATE TABLE Relation_1(
    k integer,
    val integer
    Primary Key (k)
    )'''
    main([sql_CT])
    for i in range(1000):
        sql_I = "INSERT INTO Relation_1 (k, val) VALUES (" + str(i+1) +", " + str(i+1) + ")"
        readQuery([sql_I])

def table_2():
    sql_CT = '''CREATE TABLE Relation_2(
    k integer,
    val integer
    Primary Key (k)
    )'''
    readQuery([sql_CT])
    for i in range(1000):
        sql_I = "INSERT INTO Relation_2 (k, val) VALUES (" + str(i+1) +", 1)"
        readQuery([sql_I])

def table_3():
    sql_CT = '''CREATE TABLE Relation_3(
    k integer,
    val integer
    Primary Key (k)
    )'''
    readQuery([sql_CT])
    for i in range(10000):
        sql_I = "INSERT INTO Relation_3 (k, val) VALUES (" + str(i+1) +", " + str(i+1) + ")"
        readQuery([sql_I])
    
def table_4():
    sql_CT = '''CREATE TABLE Relation_4(
    k integer,
    val integer
    Primary Key (k)
    )'''
    readQuery([sql_CT])
    for i in range(10000):
        sql_I = "INSERT INTO Relation_4 (k, val) VALUES (" + str(i+1) +", 1)"
        readQuery([sql_I])

def table_5():
    sql_CT = '''CREATE TABLE Relation_5(
    k integer,
    val integer
    Primary Key (k)
    )'''
    readQuery([sql_CT])
    for i in range(100000):
        sql_I = "INSERT INTO Relation_5 (k, val) VALUES (" + str(i+1) +", " + str(i+1) + ")"
        readQuery([sql_I])


def table_6():
    sql_CT = '''CREATE TABLE Relation_6(
    k integer,
    val integer
    Primary Key (k)
    )'''
    readQuery([sql_CT])
    for i in range(100000):
        sql_I = "INSERT INTO Relation_6 (k, val) VALUES (" + str(i+1) +", 1)"
        readQuery([sql_I])




def all_table ():
    start = time.time()
    
    table_1()
    print("1 done")
    end1 = time.time()
    print(end1 - start)
    
    table_2()
    print("2 done")
    end2 = time.time()
    print(end2 - end1)
    
    table_3()
    print("3 done")
    end3 = time.time()
    print(end3- end2)
    
    table_4()
    print("4 done")
    end4 = time.time()
    print(end4 - end3)
    
    table_5()
    print("5 done")
    end5 = time.time()
    print(end5 - end4)
    
    table_6()
    print("6 done")
    end6 = time.time()
    print(end6 - end5)

all_table()



#print(readQuery([sql_u]))