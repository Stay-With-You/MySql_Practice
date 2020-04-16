import pymysql

'''
# 打开数据库连接
db = pymysql.connect("localhost","root","025631","database_testdb" )

# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()

# 使用 execute() 方法执行 SQL，如果表存在则删除
cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")

# 使用预处理语句创建表
sql = """CREATE TABLE EMPLOYEE (
         FIRST_NAME  CHAR(20) NOT NULL,
         LAST_NAME  CHAR(20),
         AGE INT,
         SEX CHAR(1),
         INCOME FLOAT )"""

cursor.execute(sql)

# 关闭数据库连接
db.close()

# 打开数据库连接
db = pymysql.connect("localhost", "root", "025631", "database_testdb")

# 使用cursor()方法获取操作游标
cursor = db.cursor( )

# SQL 插入语句
sql = """INSERT INTO EMPLOYEE(FIRST_NAME,
         LAST_NAME, AGE, SEX, INCOME)
         VALUES ('Mac', 'Mohan', 20, 'M', 2000)"""
try:
	# 执行sql语句
	cursor.execute(sql)
	# 提交到数据库执行
	db.commit( )
except:
	# 如果发生错误则回滚
	db.rollback( )

# 关闭数据库连接
db.close( )

# 打开数据库连接
db = pymysql.connect("localhost", "root", "025631", "database_testdb")

# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor( )

# 使用 execute()  方法执行 SQL 查询
cursor.execute("SELECT VERSION()")

# 使用 fetchone() 方法获取单条数据.
data = cursor.fetchone( )

print("Database version : %s " % data)

# 关闭数据库连接
db.close( )

# 打开数据库连接
db = pymysql.connect("localhost", "root", "025631", "database_testdb")

# 使用cursor()方法获取操作游标
cursor = db.cursor( )

# SQL 查询语句
sql = "SELECT * FROM EMPLOYEE \
       WHERE INCOME > %s" % (1000)
try:
	# 执行SQL语句
	cursor.execute(sql)
	# 获取所有记录列表
	results = cursor.fetchall( )
	for row in results:
		fname = row[0]
		lname = row[1]
		age = row[2]
		sex = row[3]
		income = row[4]
		# 打印结果
		print("fname=%s,lname=%s,age=%s,sex=%s,income=%s" % \
		      (fname, lname, age, sex, income))
except:
	print("Error: unable to fetch data")

# 关闭数据库连接
db.close( )

# 打开数据库连接
db = pymysql.connect("localhost", "root", "025631", "database_testdb")

# 使用cursor()方法获取操作游标
cursor = db.cursor( )

# SQL 删除语句
sql = "DELETE FROM EMPLOYEE WHERE AGE > %s" % (20)
try:
	# 执行SQL语句
	cursor.execute(sql)
	# 提交修改
	db.commit( )
except:
	# 发生错误时回滚
	db.rollback( )

# 关闭连接
db.close( )
'''



def connectDB():
    print('连接到mysql服务器...')
    db = pymysql.connect(
        host="localhost",
        user="root",
        passwd="025631",
        port=3306,
        db="database_testdb",
        charset='utf8',
        cursorclass=pymysql.cursors.DictCursor)
    print('连接上了!')
    return db

def creatTable():
	db = pymysql.connect("localhost", "root", "025631", "database_testdb")
	cursor = db.cursor( )
	cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")
	for num in range(1, 3):
		sql = """CREATE TABLE EMPLOYEE (
				 AGE INT PRIMARY KEY NOT NULL UNIQUE auto_increment,
		         FIRST_NAME CHAR(20),
		         LAST_NAME CHAR(20),
		         SEX CHAR(1),
		         INCOME FLOAT)"""
	cursor.execute(sql)
	db.close( )

def insertData01():
	db = pymysql.connect("localhost", "root", "025631", "database_testdb")
	cursor = db.cursor( )
	sql = """INSERT INTO EMPLOYEE(AGE, FIRST_NAME,
	         LAST_NAME, SEX, INCOME)
	         VALUES (18, 'Elon', 'Mohan', 'M', 250000)"""
	try:
		cursor.execute(sql)
		db.commit( )
	except:
		db.rollback( )
	db.close( )

def insertData02():
	db = pymysql.connect("localhost", "root", "025631", "database_testdb")
	cursor = db.cursor( )
	sql = """INSERT INTO EMPLOYEE(AGE, FIRST_NAME,
	         LAST_NAME, SEX, INCOME)
			 VALUES (17, 'Owen', 'Mohan', 'M', 230000)"""
	try:
		cursor.execute(sql)
		db.commit( )
	except:
		db.rollback( )
	db.close( )

def searchDB():
	db = pymysql.connect("localhost", "root", "025631", "database_testdb")
	cursor = db.cursor( )
	sql = "SELECT * FROM EMPLOYEE \
	       WHERE INCOME > %s" % (1000)
	try:
		cursor.execute(sql)
		results = cursor.fetchall( )
		for row in results:
			fname = row[0]
			lname = row[1]
			age = row[2]
			sex = row[3]
			income = row[4]
			print("age = %s, fname = %s,lname = %s,sex = %s,income = %s" % \
			      ( age, fname, lname, sex, income))
			db.commit( )
	except:
		print("Error: unable to fetch data")
		db.rollback( )
	db.close( )

def delDB():
	db = pymysql.connect("localhost", "root", "025631", "database_testdb")
	cursor = db.cursor( )
	sql = "DELETE FROM EMPLOYEE WHERE SEX = M"
	try:
		count = cursor.execute(sql)
		print(count)
		db.commit( )
	except:
		print("Error: unable to fetch data")
		db.rollback( )
	db.close( )

def changeData():
	db = pymysql.connect("localhost", "root", "025631", "database_testdb")
	cursor = db.cursor( )
	sql = "UPDATE EMPLOYEE SET AGE = AGE + 1 WHERE SEX = '%c'" % ('M')
	try:
		cursor.execute(sql)
		db.commit( )
	except:
		db.rollback( )
	db.close( )

def sortDB():
	db = pymysql.connect("localhost", "root", "025631", "database_testdb")
	cursor = db.cursor( )
	sql = "SELECT * FROM EMPLOYEE ORDER BY row[0]"
	try:
		cursor.execute(sql)
		db.commit( )
	except:
		db.rollback( )
	db.close( )




connectDB()
creatTable()
insertData01()
searchDB()
print("\n")
changeData()
searchDB()
print("\n")
insertData02()
searchDB()
print("\n")
sortDB()  # ORDER
searchDB()

