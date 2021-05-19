from yazaki_packages.db import PsDb, OraDB
from yazaki_packages.cloud import SplCloud
from yazaki_packages.logs import Logging, DBLogging
from datetime import datetime
import pathlib
import sys

from dotenv import load_dotenv
app_path = f'{pathlib.Path().absolute()}'
# app_path = f"/home/seiwa/webservice/sync_service"
env_path = f"{app_path}/.env"

load_dotenv(env_path)


def read_db(keys):
    docs = PsDb().get_fetch_all(f"select id,case when substring(receive_no, 1, 2) = 'TI' then 'INJ' else 'AW' end factory ,receive_date,receive_no from tbt_receive_headers where id='{keys}'")
    
    rec_no = None
    i = 0
    while i < len(docs):
        r = docs[i]
        rec_id = r[0]
        rec_tag = r[1]
        rec_date = str(r[2])
        rec_no = r[3]

        unit_name = "COIL"
        if rec_tag == "INJ":
            unit_name = "PART"

        sql_body = f"""select '{rec_no}',b.seq,pm.partno,b.plan_qty,b.plan_ctn,'C','{unit_name}','10','{rec_tag}',pm.description,b.id
        from tbt_receive_bodys b 
        inner join tbt_parts p on b.part_id =p.id 
        inner join tbt_part_masters pm on p.part_id=pm.id
        where b.receive_id='{rec_id}'"""
        
        doc_body = PsDb().get_fetch_all(sql_body)
        rec_check = OraDB().get_fetch_one(f"select RECEIVINGKEY from TXP_RECTRANSENT where RECEIVINGKEY='{rec_no}'")

        ### delete older data
        OraDB().excute_data(f"DELETE TXP_RECTRANSENT WHERE RECEIVINGKEY='{rec_no}'  AND RECPLNCTN= 0")
        OraDB().excute_data(f"DELETE TXP_RECTRANSBODY WHERE RECEIVINGKEY='{rec_no}' AND RECCTN=0")


        plnctn = 0
        for j in doc_body:
            running_seq = j[1]
            part_no = j[2]
            rec_body_qty = int(j[3])
            rec_body_pln_ctn = int(j[4])
            unit_title  = j[6]
            coils_name = 20
            part_desc = str(j[9]).replace("'", "''")

            plnctn += int(j[4])

            sql_part = OraDB().get_fetch_one(f"select partno from txp_part where partno='{part_no}'")
            sql_insert_part = f"update txp_part set partname='{part_desc}' where partno='{part_no}'"
            if sql_part != False:
                OraDB().excute_data(sql_insert_part)
        
            check_body_duplicate = OraDB().get_fetch_one(f"SELECT * FROM TXP_RECTRANSBODY WHERE receivingkey='{j[0]}' AND PARTNO='{j[2]}'")
            sql_rec_body = f"UPDATE TXP_RECTRANSBODY SET PLNQTY='{rec_body_qty}', PLNCTN='{rec_body_pln_ctn}' WHERE receivingkey='{j[0]}' AND PARTNO='{j[2]}'"
            if check_body_duplicate is False:
                rvno = OraDB().get_fetch_one(f"(select 'BD'|| TO_CHAR(sysdate,'yyMMdd') || replace(to_char(emp_TXP__RCMANGENO_CK2.nextval,'00099'),' ','') as genrunno  from dual)")
                sql_rec_body = (f"""INSERT INTO TXP_RECTRANSBODY
                                (RECEIVINGKEY, RECEIVINGSEQ, PARTNO, PLNQTY, PLNCTN,RECQTY,RECCTN,TAGRP, UNIT, CD, WHS, DESCRI, RVMANAGINGNO,UPDDTE, SYSDTE, CREATEDBY,MODIFIEDBY,OLDERKEY)
                                VALUES('{rec_no}', '{running_seq}', '{part_no}', {rec_body_qty}, {rec_body_pln_ctn},0,0,'C', '{unit_title}','{coils_name}' , '{rec_tag}','{part_desc}', '{rvno}',sysdate, sysdate, 'SKTSYS', 'SKTSYS', '{rec_no}')""")

            if OraDB().excute_data(sql_rec_body):
                PsDb().excute_data(f"update tbt_receive_bodys set sync=true where id='{j[10]}'")

            print(j)

        sql_insert_ent = f"""UPDATE TXP_RECTRANSENT SET RECEIVINGMAX='{len(doc_body)}',RECPLNCTN='{plnctn}' WHERE RECEIVINGKEY='{rec_no}'"""
        if rec_check is False:
            sql_insert_ent = f"""INSERT INTO TXP_RECTRANSENT(RECEIVINGKEY, RECEIVINGMAX, RECEIVINGDTE, VENDOR, RECSTATUS, RECISSTYPE, RECPLNCTN,RECENDCTN, UPDDTE, SYSDTE)
            VALUES('{rec_no}', {len(doc_body)}, to_date('{rec_date}', 'YYYY-MM-DD'), '{rec_tag}', 0, '01', '{plnctn}',0, current_timestamp, current_timestamp)"""

        OraDB().excute_data(sql_insert_ent)

        
        PsDb().excute_data(f"update tbt_receive_headers set sync=true where id='{rec_id}'")
        print(r)
        print(f"==========================================================================")

        main_item = OraDB().get_fetch_one(f"SELECT count(PLNCTN)||'/'||sum(PLNCTN) FROM TXP_RECTRANSBODY WHERE receivingkey='{rec_no}'")
        sub_item = PsDb().get_fetch_one(f"select count(t.plan_ctn)||'/'||sum(t.plan_ctn) from tbt_receive_bodys t where t.receive_id='{rec_id}'")
        msg = f"FACTORY: {rec_tag}\nRECEIVENO: {rec_no}\MAIN: {main_item} GEDI: {sub_item}\nAT: {datetime.now().strftime('%Y-%m-%d %X')}"
        SplCloud().linenotify(msg)
        i += 1

def check_db_sync():
    f = False
    sql = f"""select t.id,t.receive_no from tbt_receive_headers t 
    inner join tbt_receive_bodys b on t.id = b.receive_id 
    where b.sync=false
    group by t.id,t.receive_no"""
    doc = PsDb().get_fetch_all(sql)

    for i in doc:
        PsDb().excute_data(f"update tbt_receive_headers set sync=false where id='{i[0]}'")
        DBLogging("SYSTEM" , f"{i[0]} SYNC {i[1]}", "ERROR")

    if len(doc) > 0:
        f = True

    return f

def main():
    sql = f"""select t.id from tbt_receive_headers t 
    inner join tbt_receive_bodys b on t.id = b.receive_id 
    where b.sync=false
    group by t.id"""
    doc = PsDb().get_fetch_all(sql)

    for i in doc:
        read_db(i[0])

if __name__ == '__main__':
    main()
    if check_db_sync():
        main()
    sys.exit(0)