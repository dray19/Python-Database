import psycopg2

class Redshift:
    def __init__(self, dbname = '', host = '', port = '', user = '', password = ''):
        con_str = "dbname='{}' " \
                  "host='{}'" \
                  " port='{}' " \
                  "user='{}'" \
                  " password='{}'".format(dbname, host, port, user, password)
        self.conn = psycopg2.connect(con_str)

    def load_sql(self, file):
        cursor = self.conn.cursor()
        sql_file = open(file, 'r')
        cursor.execute(sql_file.read())
        self.conn.commit()
        cursor.close()

    def copy(self, table, s3_path,aws_access):
        cursor = self.conn.cursor()
        sql = """copy {} 
        from 's3://drazenzack-bucket/{}.csv' 
        credentials 'aws_iam_role={}' 
        delimiter ','region 'us-east-1';""" .format(table, s3_path, aws_access)
        cursor.execute(sql)
        self.conn.commit()
        cursor.close()

    def execute(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        return results
    def close_con(self):
        self.conn.close()

red_one = Redshift(dbname = 'zagdb',host = 'drazen-cluster.cixyc346y5zu.us-east-1.redshift.amazonaws.com', port = '5439',user = 'dray19', password = 'Mancity1')
red_one.load_sql('zagdb.sql')
######################
names = ['category','region', 'vendor', 'product', 'store', 'customer','salestransaction','soldvia']
for i in names:
    red_one.copy(table=i, s3_path=i, aws_access='arn:aws:iam::029046668595:role/dray_ds530')
#######################
red_one.execute("select * from category;")
red_one.execute("select * from store;")
red_one.close_con()
#####################################

