import duckdb
from pprint import pprint
from sql_formatter.core import format_sql
from db.schema import Schema

if __name__ == '__main__':
    con = duckdb.connect()
    rel = duckdb.read_csv("./data/granola.csv", header=True, normalize_names=True, date_format="%d/%m/%Y")

    names = rel.columns
    names = [(name, name.replace(' ', '_')) for name in names]
    print("cols =", names)
    projection = ", ".join([f'"{old}" as {new}' for old, new in names])
    rel = rel.project(projection)
    rel.create("granola")

    duckdb.sql("SELECT * from granola limit 5").show()


    dims = {"geo" : ["storename", "city"], "time" : ["_month", "_year"], "product":["product"]}
    measures = [("number", "sum")]
    sch = Schema("granola", dims, measures)
    print(sch)