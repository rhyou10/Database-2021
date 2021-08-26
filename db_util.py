import json
import pymysql
import pandas as pd

with open('data/mysql.json', 'r') as file: 
    config_str = file.read() 
    config = json.loads(config_str)
    
#   conn.commit()


# product db 만들기  pid, 제품명, 가격, 카테고리, 원가
def mk_pdb():    
    sql = """
    CREATE TABLE if NOT EXISTS  products(
    pid INT NOT NULL PRIMARY key AUTO_INCREMENT,
    pname VARCHAR(20) NOT NULL,
    pprice INT,
    pcategory VARCHAR(20),
    pcost INT
    )AUTO_INCREMENT=1;
    """
    conn = pymysql.connect(**config)
    cur = conn.cursor()
    cur.execute(sql)
    conn.close()
    cur.close()


def dl_db(db_name):
    conn = pymysql.connect(**config)
    sql = f"DROP TABLE {db_name}"
    cur = conn.cursor()
    cur.execute(sql)
    conn.close()
    cur.close()


# sales db 만들기 sid, 만든날짜, 회사, spid, 판매개수
def mk_sdb():
    conn = pymysql.connect(**config)
    sql = """
    CREATE TABLE if NOT EXISTS sales(
    sid INT NOT NULL PRIMARY key AUTO_INCREMENT,
    sdate DATE,
    scompany VARCHAR(20),
    spid INT NOT NULL,
    sunit INT 
    )AUTO_INCREMENT=101;   
    """
    cur = conn.cursor()
    cur.execute(sql)
    conn.close()
    cur.close()



#insert
def i_pdb(p_i):
    conn = pymysql.connect(**config)
    sql = """
    INSERT INTO products(pname , pprice , pcategory , pcost) 
    VALUES(%s, %s, %s, %s)
    """
    cur = conn.cursor()
    cur.execute(sql,p_i)
    conn.commit()
    conn.close()
    cur.close()

def i_sdb(p_i):
    conn = pymysql.connect(**config)
    sql = """
    INSERT INTO sales(sdate, scompany , spid, sunit) 
    VALUES(%s, %s, %s, %s)
    """
    cur = conn.cursor()
    cur.execute(sql,p_i)
    conn.commit()
    conn.close()
    cur.close()

def get_db(sql):
    conn = pymysql.connect(**config)
    cur = conn.cursor()
    cur.execute(sql)    
    x = cur.fetchall()
    df = pd.DataFrame(x)
    conn.close()
    cur.close()
    return df

