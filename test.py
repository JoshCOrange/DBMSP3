#import sqlvalidator
from BTrees.IIBTree import IIBTree
import BTrees
import re
import pandas as pd
import ast
#print(help(BTrees))
#print(help(BTrees.IOBTree))
#print(help(BTrees.OIBTree))

#from bplustree import BPlusTree
#import bplustree
#print(help(bplustree))


def readDict(schemaDict):
    '''dic = {"Name": "Zara", "Age": ["a", "b","c"]}
    #print "Equivalent String : %s" % str (dict)
    with open("dict_test.txt", "w") as f:
        f.write(str (dic)) STORE IN str(dict)!!!!!!!!!!!!!!
    with open("dict_test.txt", "r") as f:
        t = f.read()'''
     
    return ast.literal_eval(schemaDict)
schemaDict = {
    "table_name": "ABC",
    "primary_key": ["name_x1", "name_x2"],
    "column_name": ["name_x1", "name_x2", "name_x3", "name_x4"],
    "column_type": ["integer", "string", "integer", "integer"],
    "foreign_key": ["name_x3", "name_x4"],
    "foreign_table": ["T_1", "T_2"],
    "foreign_column": ["C_1", "C_2"],
    "foreign_delete": ["S_1", "S_2"]
}
print(type(readDict(str(schemaDict))))

exit()



#reference: https://stackoverflow.com/questions/47052/what-code-would-i-use-to-convert-a-sql-like-expression-to-a-regex-on-the-fly
def LIKE(str): 
    regex_pattern = "^" + re.sub(
        "[%_]|\[[^]]*\]|[^%_[]+",
        lambda match:
        (".*" if match.group() == "%"
        else "." if match.group() == "_"
        else match.group() if match.group().startswith("[") and match.group().endswith("]")
        else re.escape(match.group())), str
        ) + "$"
    print(regex_pattern)
    return regex_pattern

#reference: https://stackoverflow.com/questions/32614357/search-and-filter-pandas-dataframe-with-regular-expressions
def Liketest(condition): #str = LIKE 的condition
    reg = LIKE(condition)
    df = pd.DataFrame(
        {
            'col1': ['vhigh', 'low', 'vlow'],
            'col2': ['eee', 'low', 'high'],
            'val': [100,200,300]
        }
    )
    mask = df[['col1']].apply(
        lambda x: x.str.contains(
            reg,
            regex=True
        )
    ).any(axis=1)
    return (df[mask]) #return the all rows that fit the condition


#print(Liketest("%")) #test for LIKE clause



# Import pandas package
import pandas as pd

# Define a dictionary containing employee data
data = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'],
		'Age':[27, 24, 22, 32],
		'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
		'Qualification':['Msc', 'MA', 'MCA', 'Phd']}

# Convert the dictionary into DataFrame
df = pd.DataFrame(data)
row = df.iloc[1]
#print(row)

# select two columns
a = ['Name', 'Qualification']
print(row[a])

exit()


'''formatted_sql = sqlvalidator.format_sql("SELECT Employees.LastName, COUNT(Orders.OrderID) AS NumberOfOrders FROM (Orders INNER JOIN Employees ON Orders.EmployeeID = Employees.EmployeeID) GROUP BY LastName HAVING COUNT(Orders.OrderID) > 10 LIMIT 10;")

sql_query = sqlvalidator.parse("SELECT * from table")
print(sql_query.is_valid())

invalid_query = "SELECT ** FROM x;" #Fix this
sql_query = sqlvalidator.parse(invalid_query)
print(sql_query.is_valid())

with cte as (select * from a)
select * from cte'''
print(re.split("AND|aND|AnD|ANd|anD|aNd|And|and|OR|oR|Or|or", "11 And 15 oR a_num >= 20 OR p# IN (2,4,8)"))
exit()
tree = IIBTree()
tree.insert(1,2)
tree.insert(1,3)
tree.insert(1,4)
tree.insert(1,5)
#print(tree)

print(tree.get(1))

tree.pop(1)
tree.insert(1,5)

print(tree.get(1))

