import sys
import os
import cx_Oracle as o
import pathlib
from datetime import datetime
from yazaki_packages.db import OraDB
from yazaki_packages.cloud import SplCloud

from dotenv import load_dotenv
app_path = f'{pathlib.Path().absolute()}'
# app_path = f"/home/seiwa/webservice/sync_service"
env_path = f"{app_path}/.env"
load_dotenv(env_path)

def main():
    ora = o.connect(os.getenv("ORA_STR"))
    cur = ora.cursor()
    cur.execute(f"SELECT ID,KEY,REC_NO FROM TMP_RECEIVEMERGE WHERE SYNC=0")
    obj = cur.fetchall()
    for i in obj:
        # get rnd number
        rnd = str(i[1]).strip().split(",")
        keys = []
        for b in rnd:
            n = str(b).strip().replace(" ", "")
            keys.append(int(n[len("TI20210524"):]))

        xkeys = str(keys).replace('[', '').replace(']', '').replace("'", "")

        # strings = [str(integer) for integer in keys]
        # a_string = "".join(strings)
        # an_integer = int(a_string)
        # rkeys = str(str(i[2]).strip())[:2] + "{:08d}".format(an_integer)
        # end

        receive_key = str(str(i[1]).strip().split(",")).replace("[", "").replace("]", "").replace(" ", "")
        sql_body = f"""SELECT '{str(i[2]).strip()}' RECENO, PARTNO,sum(PLNQTY) qty,sum(PLNCTN) ctn,UNIT,CD,WHS,DESCRI FROM TXP_RECTRANSBODY WHERE RECEIVINGKEY IN ({receive_key}) GROUP BY PARTNO,UNIT,CD,WHS,DESCRI ORDER BY PARTNO"""
        cur.execute(sql_body)
        body = cur.fetchall()
        if len(body) > 0:
            plnctn = 0
            rec_tag = ""
            x = 1
            for j in body:
                rvno = OraDB().get_fetch_one(f"(select 'BD'|| TO_CHAR(sysdate,'yyMMdd') || replace(to_char(emp_TXP__RCMANGENO_CK2.nextval,'00099'),' ','') as genrunno  from dual)")
                print(f"{rvno} => {list(j)}")
                partname = str(j[7]).replace("'", "''")
                sql_rec_body = (f"""INSERT INTO TXP_RECTRANSBODY
                                    (RECEIVINGKEY, RECEIVINGSEQ, PARTNO, PLNQTY, PLNCTN,RECQTY,RECCTN,TAGRP, UNIT, CD, WHS, DESCRI, RVMANAGINGNO,UPDDTE, SYSDTE, CREATEDBY,MODIFIEDBY,OLDERKEY)
                                    VALUES('{j[0]}', '{x}', '{j[1]}', {j[2]}, {j[3]},0,0,'C', '{j[4]}','{j[5]}' , '{j[6]}','{partname}', '{rvno}',sysdate, sysdate, 'SKTSYS', 'SKTSYS', '{xkeys}')""")
                plnctn += int(str(j[3]))
                rec_tag = str(j[6])
                cur.execute(sql_rec_body)
                x += 1

            sql_insert_ent = f"""INSERT INTO TXP_RECTRANSENT(RECEIVINGKEY, RECEIVINGMAX, RECEIVINGDTE, VENDOR, RECSTATUS, RECISSTYPE, RECPLNCTN,RECENDCTN, UPDDTE, SYSDTE)
            VALUES('{str(i[2]).strip()}', {len(body)}, to_date('{datetime.now().strftime('%Y-%m-%d')}', 'YYYY-MM-DD'), '{rec_tag}', 0, '01', '{plnctn}',0, current_timestamp, current_timestamp)"""
            cur.execute(sql_insert_ent)

            # delete after merge
            cur.execute(f"DELETE TXP_RECTRANSBODY WHERE RECEIVINGKEY IN ({receive_key})")
            cur.execute(f"DELETE TXP_RECTRANSENT WHERE RECEIVINGKEY IN ({receive_key})")

        ## delete temp data
        cur.execute(f"DELETE TMP_RECEIVEMERGE WHERE ID='{str(i[0])}'")
        print(f"DELETE TMP_RECEIVEMERGE ID => '{str(i[0])}'")
        ora.commit()
        if len(body) > 0:
            msg = f"MERGE RECEIVE({rec_tag})\nRECEIVENO: {str(i[2]).strip()}\nITEM: {len(body)} CTN: {plnctn}\nFROM: {xkeys}\nAT: {datetime.now().strftime('%Y-%m-%d %X')}"
            SplCloud().linenotify(msg)

    cur.close()
    ora.close()

if __name__ == "__main__":
    main()
    sys.exit(0)