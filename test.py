import sqlvalidator
import BTrees
import bplustree
#print(help(BTrees))
#print(help(BTrees.IOBTree))
print(help(bplustree.tree))
'''formatted_sql = sqlvalidator.format_sql("SELECT Employees.LastName, COUNT(Orders.OrderID) AS NumberOfOrders FROM (Orders INNER JOIN Employees ON Orders.EmployeeID = Employees.EmployeeID) GROUP BY LastName HAVING COUNT(Orders.OrderID) > 10 LIMIT 10;")

sql_query = sqlvalidator.parse("SELECT * from table")
print(sql_query.is_valid())

invalid_query = "SELECT ** FROM x;" #Fix this
sql_query = sqlvalidator.parse(invalid_query)
print(sql_query.is_valid())

with cte as (select * from a)
select * from cte'''