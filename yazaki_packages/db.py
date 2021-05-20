class PsDb:
    def __init__(self, *args, **kwargs):
        return None

    def get_fetch_one(self, sql):
        import os
        import psycopg2
        from yazaki_packages.logs import DBLogging
        
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
            DBLogging("WMS_DB" , f"FETCH ONE", str(e))
            cur.close()

        return i

    def get_fetch_all(self, sql):
        import os
        import psycopg2
        from yazaki_packages.logs import DBLogging

        obj = None
        try:
            conn = psycopg2.connect(host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT"), database=os.getenv(
                "DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWD"))
            cur = conn.cursor()
            cur.execute(sql)
            obj = cur.fetchall()
        except Exception as ex:
            print(ex)
            DBLogging("PS_DB" , f"FETCH ALL ERROR", str(ex))
            pass
        return obj

    def excute_data(self, sql):
        import os
        import psycopg2
        from yazaki_packages.logs import DBLogging

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
            DBLogging("PS_DB" , f"ERROR EXCUTE", str(e))
            conn.rollback()
            pass

        cur.close()
        conn.close()
        return True


class OraDB:
    def __init__(self, *args, **kwargs):
        return None

    def get_fetch_one(self, sql):
        import os
        import cx_Oracle
        from yazaki_packages.logs import DBLogging

        conn = cx_Oracle.connect(os.getenv("ORA_STR"))
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

        except Exception as e:
            print(sql)
            print(str(e))
            DBLogging("ORA_DB" , f"ERROR FETCH ONE", str(e))
        
        cur.close()
        return i

    def get_fetch_all(self, sql):
        import os
        import cx_Oracle
        from yazaki_packages.logs import DBLogging

        try:
            conn = cx_Oracle.connect(os.getenv("ORA_STR"))
            cur = conn.cursor()
            cur.execute(sql)
            obj = cur.fetchall()
        except Exception as ex:
            print(ex)
            DBLogging("ORA_DB" , f"ERROR FETCH ALL", str(ex))
            pass
        return obj

    def excute_data(self, sql):
        import os
        import cx_Oracle
        from yazaki_packages.logs import DBLogging

        f = True
        conn = cx_Oracle.connect(os.getenv("ORA_STR"))
        cur = conn.cursor()
        try:
            cur.execute(sql)
            conn.commit()
            pass
        except Exception as e:
            print(sql)
            print(str(e))
            f = False
            DBLogging("ORA_DB" , f"ERROR EXCUTE", str(e))
            conn.rollback()
            pass

        cur.close()
        conn.close()
        return f

class OraFG:
    def __init__(self, *args, **kwargs):
        return None

    def get_fetch_one(self, sql):
        import os
        import cx_Oracle
        from yazaki_packages.logs import DBLogging

        conn = cx_Oracle.connect(os.getenv("ORA_FG"))
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

        except Exception as e:
            print(sql)
            print(str(e))
            DBLogging("ORA_FG" , f"ERROR FETCH ONE", str(e))
        
        cur.close()
        return i

    def get_fetch_all(self, sql):
        import os
        import cx_Oracle
        from yazaki_packages.logs import DBLogging

        try:
            conn = cx_Oracle.connect(os.getenv("ORA_FG"))
            cur = conn.cursor()
            cur.execute(sql)
            obj = cur.fetchall()
        except Exception as ex:
            print(ex)
            DBLogging("ORA_FG" , f"ERROR FETCH ALL", str(ex))
            pass
        return obj

    def excute_data(self, sql):
        import os
        import cx_Oracle
        from yazaki_packages.logs import DBLogging

        f = True
        conn = cx_Oracle.connect(os.getenv("ORA_FG"))
        cur = conn.cursor()
        try:
            cur.execute(sql)
            conn.commit()
            pass
        except Exception as e:
            print(sql)
            print(str(e))
            f = False
            DBLogging("ORA_FG" , f"ERROR EXCUTE", str(e))
            conn.rollback()
            pass

        cur.close()
        conn.close()
        return f

class WmsDb:
    def __init__(self, *args, **kwargs):
        return None

    def get_fetch_one(self, sql):
        import os
        import psycopg2
        from yazaki_packages.logs import DBLogging

        conn = psycopg2.connect(host=os.getenv("DB_WMS_HOST"), port=os.getenv("DB_WMS_PORT"), database=os.getenv(
            "DB_WMS_NAME"), user=os.getenv("DB_WMS_USER"), password=os.getenv("DB_WMS_PASSWD"))
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
            DBLogging("WMS_DB" , f"FETCH ONE", str(e))
            cur.close()

        return i

    def get_fetch_all(self, sql):
        import os
        import psycopg2
        from yazaki_packages.logs import DBLogging

        obj = None
        try:
            conn = psycopg2.connect(host=os.getenv("DB_WMS_HOST"), port=os.getenv("DB_WMS_PORT"), database=os.getenv(
                "DB_WMS_NAME"), user=os.getenv("DB_WMS_USER"), password=os.getenv("DB_WMS_PASSWD"))
            cur = conn.cursor()
            cur.execute(sql)
            obj = cur.fetchall()
        except Exception as ex:
            print(ex)
            DBLogging("WMS_DB" , f"FETCH ALL ERROR", str(ex))
            pass
        return obj

    def excute_data(self, sql):
        import os
        import psycopg2
        from yazaki_packages.logs import DBLogging

        conn = psycopg2.connect(host=os.getenv("DB_WMS_HOST"), port=os.getenv("DB_WMS_PORT"), database=os.getenv(
            "DB_WMS_NAME"), user=os.getenv("DB_WMS_USER"), password=os.getenv("DB_WMS_PASSWD"))
        cur = conn.cursor()
        try:
            cur.execute(sql)
            conn.commit()
            pass
        except Exception as e:
            print(sql)
            print(str(e))
            DBLogging("WMS_DB" , f"ERROR EXCUTE", str(e))
            conn.rollback()
            pass

        cur.close()
        return True
