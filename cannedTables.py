from parser import *
import time
from execution import *

def table_1():
    sql_CT = '''CREATE TABLE Relation_1(
    k integer,
    val integer,
    Primary Key (k)
    )'''
    re_tuple = readQuery([sql_CT])
    schemaDict = re_tuple[1]
    newTree = create_table(schemaDict)
    tableName = schemaDict.get('table_name')
    #for i in range(1000):
    for i in range(100):
        sql_I = "INSERT INTO Relation_1(k, val) VALUES (" + str(i+1) +", " + str(i+1) + ")"
        re_tuple = readQuery([sql_I])
        schemaDict = re_tuple[1]
        insert_table(schemaDict, newTree)
    return newTree, tableName

def table_2():
    sql_CT = '''CREATE TABLE Relation_2(
    k integer,
    val integer,
    Primary Key (k)
    )'''
    re_tuple = readQuery([sql_CT])
    schemaDict = re_tuple[1]
    newTree = create_table(schemaDict)
    tableName = schemaDict.get('table_name')
    for i in range(1000):
        sql_I = "INSERT INTO Relation_2(k, val) VALUES (" + str(i+1) +", 1)"
        re_tuple = readQuery([sql_I])
        schemaDict = re_tuple[1]
        insert_table(schemaDict, newTree)
    return newTree, tableName
def table_3():
    sql_CT = '''CREATE TABLE Relation_3(
    k integer,
    val integer,
    Primary Key (k)
    )'''
    re_tuple = readQuery([sql_CT])
    schemaDict = re_tuple[1]
    newTree = create_table(schemaDict)
    tableName = schemaDict.get('table_name')
    for i in range(10000):
        sql_I = "INSERT INTO Relation_3(k, val) VALUES (" + str(i+1) +", " + str(i+1) + ")"
        re_tuple = readQuery([sql_I])
        schemaDict = re_tuple[1]
        insert_table(schemaDict, newTree)
    return newTree, tableName
    
def table_4():
    sql_CT = '''CREATE TABLE Relation_4(
    k integer,
    val integer,
    Primary Key (k)
    )'''
    re_tuple = readQuery([sql_CT])
    schemaDict = re_tuple[1]
    newTree = create_table(schemaDict)
    tableName = schemaDict.get('table_name')
    for i in range(10000):
        sql_I = "INSERT INTO Relation_4(k, val) VALUES (" + str(i+1) +", 1)"
        re_tuple = readQuery([sql_I])
        schemaDict = re_tuple[1]
        insert_table(schemaDict, newTree)
    return newTree, tableName

def table_5():
    sql_CT = '''CREATE TABLE Relation_5(
    k integer,
    val integer,
    Primary Key (k)
    )'''
    re_tuple = readQuery([sql_CT])
    schemaDict = re_tuple[1]
    newTree = create_table(schemaDict)
    tableName = schemaDict.get('table_name')
    for i in range(100000):
        sql_I = "INSERT INTO Relation_5(k, val) VALUES (" + str(i+1) +", " + str(i+1) + ")"
        re_tuple = readQuery([sql_I])
        schemaDict = re_tuple[1]
        insert_table(schemaDict, newTree)
    return newTree, tableName


def table_6():
    sql_CT = '''CREATE TABLE Relation_6(
    k integer,
    val integer,
    Primary Key (k)
    )'''
    re_tuple = readQuery([sql_CT])
    schemaDict = re_tuple[1]
    newTree = create_table(schemaDict)
    tableName = schemaDict.get('table_name')
    for i in range(1000):
        sql_I = "INSERT INTO Relation_6(k, val) VALUES (" + str(i+1) +", 1)"
        re_tuple = readQuery([sql_I])
        schemaDict = re_tuple[1]
        insert_table(schemaDict, newTree)
    return newTree, tableName






#print(readQuery([sql_u]))