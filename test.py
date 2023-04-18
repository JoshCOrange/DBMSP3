#import sqlvalidator
from BTrees.IIBTree import IIBTree
import BTrees
#print(help(BTrees))
#print(help(BTrees.IOBTree))
#print(help(BTrees.IIBTree))

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


tree = IIBTree()
tree.insert(1,2)
tree.insert(1,3)
tree.insert(1,4)
tree.insert(1,5)

print(len(tree))