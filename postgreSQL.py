import psycopg2
import csv
class POST:
    def __init__(self, dbname, host="localhost", port="PGSQL_LOCAL_PORT", user="postgres", password=''):
        con_str = "dbname='{}' host='{}' port='{}' user='{}' password='{}'".format(dbname, host, port, user, password)
        self.conn = psycopg2.connect(con_str)
        self.conn.autocommit = True
    def table_to_csv(self, table_name):
        file_name = "{}.csv".format(table_name)
        query = "select * from zg.%s;"

        cursor = self.conn.cursor()
        cursor.execute(query % table_name)
        rows = cursor.fetchall()

        with open(file_name, 'w') as f:
            writer = csv.writer(f, delimiter=',')
            for row in rows:
                writer.writerow(row)

        cursor.close()

        return True
    def create_database(self, dbname):
        cursor = self.conn.cursor()

        cursor.execute("create database %s;" % dbname)
        cursor.close()
    def execute(self, query):
        cursor = self.conn.cursor()

        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        return results



psql = POST("zagdb", host="127.0.0.1",port = "5432", user = "postgres", password='Mancity1')
tables = ['product', 'customer', 'category','region','salestransaction','soldvia','store','vendor']
for i in tables:
    psql.table_to_csv(i)


