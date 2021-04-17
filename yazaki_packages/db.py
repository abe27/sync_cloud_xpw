class Db:
    def __init__(self, *args, **kwargs):
        return None
    
    def get_fetch_one(self, sql):
        import os
        import psycopg2
        conn = psycopg2.connect(host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"), database=os.getenv(
            "DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWD"))
        i = 0
        cur = conn.cursor()
        try:
            cur.execute(sql)
            db = cur.fetchone()
            # print(type(db))
            if db != None:
                i = db[0]
            else:
                i = False

            cur.close()
        except Exception as e:
            print(sql)
            print(str(e))
            cur.close()

        return i

    def get_fetch_all(self, sql):
        import os
        import psycopg2
        try:
            conn = psycopg2.connect(host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"), database=os.getenv(
                "DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWD"))
            cur = conn.cursor()
            cur.execute(sql)
            obj = cur.fetchall()
        except Exception as ex:
            print(ex)
            pass
        return obj

    def excute_data(self, sql):
        import os
        import psycopg2
        conn = psycopg2.connect(host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"), database=os.getenv(
            "DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWD"))
        cur = conn.cursor()
        try:
            cur.execute(sql)
            conn.commit()
            pass
        except Exception as e:
            print(sql)
            print(str(e))
            conn.rollback()
            pass

        cur.close()
        return True
    