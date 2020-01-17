import pymysql
# class for database operation
class mysqlHandler(object):
    # class initialization
    def __init__(self):
        self.host = 'localhost'
        self.port = 3306  # port number 
        self.user = 'root'  # username
        self.password = ""  # password
        self.db = "test"  # database name 
        self.table = "news"  # table name

    # connect to db
    def connectMysql(self):
        try:
            self.con = pymysql.connect(
                host=self.host, port=self.port, user=self.user,
                                        passwd=self.password, db=self.db, charset='utf8')
            self.cursor = self.con.cursor()
        except:
            print('connect mysql error.')

    # check if db and table exist and create them if not
    def createDbAndTable(self):
        dbCreationSql = """CREATE DATABASE IF NOT EXISTS %s """ % (self.db)
        tableCreationSql = """CREATE TABLE IF NOT EXISTS %s ( \
                                    id SMALLINT, \
                                    title VARCHAR(50), \
                                    url VARCHAR(200), \
                                    img_path VARCHAR(200)) \
                                    DEFAULT charset=utf8
                                """ % (self.table)
        dbRes = self.cursor.execute(dbCreationSql)
        tableRes = self.cursor.execute(tableCreationSql)

    # insert record if record is not in table
    def insertData(self, my_dict):
        table = self.table
        sqlExit = """SELECT url FROM {0} WHERE url = '{1}'""".format(table, my_dict['url'])
        res = self.cursor.execute(sqlExit)
        # res means number of searched records in table, if > 0 then means record already exists in table
        if res:
            print("record already exists", res)
            return 0
        try:
            cols = ', '.join(my_dict.keys())
            values = '"," '.join(my_dict.values())
            sql = """INSERT INTO {0} ({1}) VALUES ({2})""".format(table, cols, '"' + values + '"')
            try:
                result = self.cursor.execute(sql)
                insert_id = self.con.insert_id()
                self.con.commit()
                # check if insertion succeeded
                if result:
                    print("News inserted successfully", self.getLastId())
                    return 1
            except pymysql.Error as e:
                # rollback when error happens
                self.con.rollback()
        except pymysql.Error as e:
            print("error happened in db，due to %d: %s" % (e.args[0], e.args[1]))

    # check largest id number
    def getLastId(self):
        sql = "SELECT max(id) FROM " + self.table
        try:
            self.cursor.execute(sql)
            # retrieve the first record
            row = self.cursor.fetchone()
            if row[0]:
                return row[0]
            # else happens when table is empty
            else:
                return 0
        except:
            print(sql + ' execute failed.')

    def closeMysql(self):
        self.cursor.close()
        self.con.close()
