#import sqlvalidator
from BTrees.OIBTree import OIBTree
import BTrees
import re
#print(help(BTrees))
#print(help(BTrees.IOBTree))
print(help(BTrees.OIBTree))

#from bplustree import BPlusTree
#import bplustree
#print(help(bplustree))


'''formatted_sql = sqlvalidator.format_sql("SELECT Employees.LastName, COUNT(Orders.OrderID) AS NumberOfOrders FROM (Orders INNER JOIN Employees ON Orders.EmployeeID = Employees.EmployeeID) GROUP BY LastName HAVING COUNT(Orders.OrderID) > 10 LIMIT 10;")

sql_query = sqlvalidator.parse("SELECT * from table")
print(sql_query.is_valid())

invalid_query = "SELECT ** FROM x;" #Fix this
sql_query = sqlvalidator.parse(invalid_query)
print(sql_query.is_valid())


with cte as (select * from a)
select * from cte'''
'''
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
'''
