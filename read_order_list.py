import pandas as pd
import os
import datetime

from yazaki_packages.db import PsDb, OraDB


fd = os.listdir("./export")
for i in fd:
    df = pd.read_excel(f'./export/{i}')
    custcode = df["CUSTCODE"][0]
    etd = datetime.datetime.strptime(str(df["ETDTAP"][0]), "%Y%m%d")
    
    sql = f"""select t.biac,t.bishpc,t.bisafn,t.factory,t.etdtap,t.shiptype,t.pono,t.partno,t.partname,t.lotno,t.balqty,t.bistdp,t.balqty/t.bistdp ctn,g.batch_file_name,t.reasoncd
            from tbt_order_datas t
            inner join tbt_gedi_datas g on t.gedi_id = g.id 
            where t.factory='AW' and t.etdtap='{etd.strftime("%Y-%m-%d")}' and t.bishpc='{custcode}' and t.shiptype='B'"""
    
    obj = PsDb().get_fetch_all(sql)
    for j in obj:
        print(list(j))
        orderno = j[6]
        partno = j[7]
        lotno = j[9]
        ctn = j[12]

        sql_checked = PsDb().get_fetch_one(f"""select t.id from tmp_checkorderlist t 
                        where t.factory='AW' and t.etdtap='{etd.strftime("%Y-%m-%d")}' and t.bishpc='{custcode}' and t.shiptype='B' and t.pono='{orderno}' and t.partno='{partno}' and t.lotno='{lotno}'""")
        
        if sql_checked is False:
            sql_insert = f"""insert into tmp_checkorderlist
                        (biac, bishpc, bisafn, factory, etdtap, shiptype, pono, partno, partname, lotno, balqty, bistdp, ctn, batch_file_name, tap_data, pg_match, ora_match, last_checked, id,remark)
                        values('{j[0]}', '{j[1]}', '{j[2]}', '{j[3]}', '{etd.strftime("%Y-%m-%d")}', '{j[5]}', '{j[6]}', '{j[7]}', '{j[8]}', '{j[9]}', {j[10]}, {j[11]}, {j[12]}, '{j[13]}', false, false, false, current_timestamp, uuid_generate_v4(), '{j[14]}')"""
            
            PsDb().excute_data(sql_insert)

    ## check data on excel
    df_count = df["No."].count()
    a = 0
    while a < df_count:
        rYPARTNO = df["YPARTNO"][a]
        rCUSTCODE = df["CUSTCODE"][a]
        rETDTAP = df["ETDTAP"][a]
        rORDERNO = df["ORDERNO"][a]
        rLOTNO = df["LOTNO"][a]
        rWireCode = df["WireCode"][a]
        rWireSymbol = df["WireSymbol"][a]
        rONPLNO = df["ONPLNO"][a]
        rQTY = df["QTY"][a]

        sql_checked = PsDb().get_fetch_one(f"""select t.id from tmp_checkorderlist t 
                        where t.factory='AW' and t.etdtap='{etd.strftime("%Y-%m-%d")}' and t.bishpc='{rCUSTCODE}' and t.shiptype='B' and t.pono='{rORDERNO}' and t.partno='{rYPARTNO}' and t.lotno='{rLOTNO}' and t.ctn='{rQTY}'""")
        
        sql_update = None
        if sql_checked is False:
            print(f"not found")
            get_cust_name = PsDb().get_fetch_one(f"select t.bisfan from tbt_customers t where t.bishpc='{rCUSTCODE}'")
            sql_update = f"""insert into tmp_checkorderlist
                        (biac, bishpc, bisafn, factory, etdtap, shiptype, pono, partno, partname, lotno, balqty, bistdp, ctn, tap_data, pg_match, ora_match, last_checked, id)
                        values('{rCUSTCODE}', '{rCUSTCODE}', '{get_cust_name}', 'AW', '{etd.strftime("%Y-%m-%d")}', 'B', '{rORDERNO}', '{rYPARTNO}', '{rYPARTNO}', '{rLOTNO}', 0, 0, {rQTY}, true, false, false, current_timestamp, uuid_generate_v4())"""

            PsDb().excute_data(sql_update)
            sql_checked = PsDb().get_fetch_one(f"""select t.id from tmp_checkorderlist t 
                        where t.factory='AW' and t.etdtap='{etd.strftime("%Y-%m-%d")}' and t.bishpc='{rCUSTCODE}' and t.shiptype='B' and t.pono='{rORDERNO}' and t.partno='{rYPARTNO}' and t.lotno='{rLOTNO}' and t.ctn='{rQTY}'""")

        else:
            sql_update = f"""update tmp_checkorderlist set tap_data=true,pg_match=true where id='{sql_checked}'"""

            PsDb().excute_data(sql_update)

        ## check ora db

        sql_checked_onora = OraDB().get_fetch_one(f"""SELECT UUID FROM TXP_ORDERPLAN t WHERE 
                    t.FACTORY='AW' AND t.BISHPC='{rCUSTCODE}' AND t.ETDTAP=TO_DATE('{etd.strftime("%Y-%m-%d")}', 'YYYY-MM-DD') AND t.SHIPTYPE='B' AND t.PONO='{rORDERNO}' AND t.PARTNO='{rYPARTNO}' AND t.LOTNO='{rLOTNO}' AND (t.BALQTY/t.BISTDP) = {rQTY}""")
        
        if sql_checked_onora != False:
            if sql_checked != False:
                sql_update = f"""update tmp_checkorderlist set ora_match=true where id='{sql_checked}'"""
                PsDb().excute_data(sql_update)

        a += 1