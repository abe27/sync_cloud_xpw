from yazaki_packages.cloud import SplCloud
from yazaki_packages.yazaki import Yk
from yazaki_packages.db import PsDb
from termcolor import colored
from datetime import datetime
import os
import pathlib
import shutil
import sys

from dotenv import load_dotenv
app_path = f'{pathlib.Path().absolute()}'
# app_path = f"/home/seiwa/webservice/sync_service"
env_path = f"{app_path}/.env"

load_dotenv(env_path)

cloud = SplCloud()
y = Yk()
db = PsDb()

def __insert_receive_ent(obj, gedi_id, tag_id, whs_id):
    rec_id = db.get_fetch_one(
        f"select id from tbt_receive_headers where receive_no='{obj['receivingkey']}'")
    if rec_id is False:
        rec_date = datetime.strptime(obj['aetodt'], '%d/%m/%Y')
        sql = f"""insert into tbt_receive_headers
              (id, gedi_id, tag_id, whs_id, receive_date, receive_no,
               receive_status, sync, created_at, updated_at)
              values(uuid_generate_v4(), '{gedi_id}', '{tag_id}', '{whs_id}', '{rec_date.strftime('%Y-%m-%d')}', '{obj['receivingkey']}', '0', false, current_timestamp, current_timestamp)"""

        db.excute_data(sql)
        rec_id = db.get_fetch_one(
            f"select id from tbt_receive_headers where receive_no='{obj['receivingkey']}'")
    return rec_id

# def check_floder():
#     folder_a = os.listdir("temp")
#     dir_name = []
#     for _i in folder_a:
#         if _i != ".gitkeep":
#             if len(os.listdir(f"temp/{_i}")) > 0:
#                 dir_name.append(_i)

#             else:
#                 os.rmdir(f"temp/{_i}")

#     return dir_name

def __download_gedi():
    # get download gedi
    obj = None
    token = cloud.get_token()
    if token != False:
        doc = cloud.download_gedi(token)
        if doc != False:
            if len(doc) > 0:
                r = doc[0]
                obj = r['data']
                i = 0
                while i < len(obj):
                    a = obj[i]
                    docs = cloud.get_text_file(
                        f"http://{os.getenv('SPL_HOSTNAME')}{a['file_path']}")
                    gedi_type = a['gedi_types']['title']
                    filename = f'./temp/{(gedi_type).upper()}'

                    # check duplicate file gedi. remove when exits.
                    if os.path.exists(filename) is False:
                        os.makedirs(filename)

                    filename += f'/{(a["batch_file_name"]).upper()}'
                    if os.path.exists(filename) == True:
                        os.remove(filename)

                    f = open(filename, mode='a')
                    for p in docs:
                        f.write((p.text).replace("\n", ""))

                    f.close()
                    # update status
                    print(colored(
                        f'download G-EDI({(a["batch_file_name"]).upper()}) completed', "yellow"))
                    cloud.linenotify_error(
                        f'download G-EDI({(a["batch_file_name"]).upper()}) completed')
                    cloud.update_gedi_status(token, a['id'], 1)
                    i += 1

        cloud.clear_token(token)

    print(colored(
        "================= end upload to spl cloud at {datetime.now()} ==================", "green"))
 
def read_gedi_folder():
    # read gedi file
    try:
        
        for i in SplCloud().check_folder("temp"):
            f_floder = f"./temp/{i}"
            list_dir = os.listdir(f_floder)
            for j in list_dir:
                print(f"{f_floder}/{j}")
                if j != ".gitkeep":

                    receive_key = None
                    keys_list = []

                    whsname = "CK2"
                    fnme = j
                    fname = f"{f_floder}/{fnme}"
                    file_id = db.get_fetch_one(
                        f"select id from tbt_gedi_datas where batch_file_name='{fnme}'")
                    if i == "RECEIVE":
                        # read receive
                        docs = y.read_ck_receive(fname)
                        a = 0
                        if len(docs) > 0:
                            whsname = f'CK{str(docs[0]["faczone"][2:])}'
                            whs_id = db.get_fetch_one(
                                f"select id from tbt_whs where title='{whsname}'")
                            tag_id = db.get_fetch_one(
                                f"select id from tbt_tag_groups where title='{docs[0]['factory']}'")
                            while a < len(docs):
                                r = docs[a]
                                if receive_key is None:
                                    receive_key =  r['receivingkey']
                                    keys_list.append(receive_key)

                                else:
                                    if receive_key !=  r['receivingkey']:
                                        keys_list.append(receive_key)

                                receive_data_id = db.get_fetch_one(
                                    f"select id from tbt_receive_datas where receivingkey='{r['receivingkey']}' and partno='{r['partno']}'")
                                if receive_data_id is False:
                                    sql_insert_recdb = f"""
                                    insert into tbt_receive_datas
                                    (id, gedi_id, tag_id, faczone, receivingkey, partno, partname, vendor, cd, unit, whs_id, tagrp, recisstype, plantype, recid, aetono, aetodt, aetctn, aetfob, aenewt, aentun, aegrwt, aegwun, aeypat, aeedes, aetdes, aetarf,
                                    aestat, aebrnd, aertnt, aetrty, aesppm, aeqty1, aeqty2, aeuntp, aeamot, plnctn, plnqty, minimum, maximum, picshelfbin, stkshelfbin, ovsshelfbin, picshelfbasicqty, outerpcs, allocateqty, sync, created_at, updated_at)
                                    values(uuid_generate_v4(), '{file_id}', '{tag_id}', '{r['faczone']}', '{r['receivingkey']}', '{r['partno']}', '{r['partname']}', '{r['vendor']}', '{r['cd']}', '{r['unit']}', '{whs_id}', '{r['tagrp']}','{r['recisstype']}','{r['plantype']}','{r['recid']}','{r['aetono']}','{r['aetodt']}','{r['aetctn']}','{r['aetfob']}','{r['aenewt']}','{r['aentun']}','{r['aegrwt']}','{r['aegwun']}','{r['aeypat']}','{r['aeedes']}','{r['aetdes']}','{r['aetarf']}','{r['aestat']}','{r['aebrnd']}','{r['aertnt']}','{r['aetrty']}','{r['aesppm']}','{r['aeqty1']}','{r['aeqty2']}','{r['aeuntp']}','{r['aeamot']}','{r['plnctn']}','{r['plnqty']}','{r['minimum']}','{r['maximum']}','{r['picshelfbin']}','{r['stkshelfbin']}','{r['ovsshelfbin']}','{r['picshelfbasicqty']}','{r['outerpcs']}','{r['allocateqty']}',true, current_timestamp, current_timestamp)
                                    """
                                    db.excute_data(sql_insert_recdb)
                                part_name = db.get_fetch_one(
                                    f"select id from tbt_part_masters where partno='{r['partno']}'")
                                part_id = db.get_fetch_one(
                                    f"select id from tbt_parts where part_id='{part_name}' and whs_id='{whs_id}'")
                                rec_id = __insert_receive_ent(
                                    r, file_id, tag_id, whs_id)
                                part_seq = db.get_fetch_one(
                                    f"select count(id) + 1 from tbt_receive_bodys where receive_id='{rec_id}'")
                                if rec_id != False:
                                    rece_body_id = db.get_fetch_one(
                                        f"select id from tbt_receive_bodys where receive_id='{rec_id}' and part_id='{part_id}'")
                                    txt_body = "update"
                                    sql_insert_body = f"update tbt_receive_bodys set plan_ctn='{r['plnctn']}',plan_qty='{r['plnqty']}' where receive_id='{rec_id}' and part_id='{part_id}'"
                                    if rece_body_id is False:
                                        txt_body = "insert"
                                        sql_insert_body = f"""insert into tbt_receive_bodys
                                            (id, receive_id, part_id, seq, plan_ctn, plan_qty,
                                            receive_ctn, sync, created_at, updated_at)
                                            values(uuid_generate_v4(), '{rec_id}', '{part_id}', '{part_seq}', '{r['plnctn']}', '{r['plnqty']}', '0', false, current_timestamp, current_timestamp)"""
                                    db.excute_data(sql_insert_body)
                                print(
                                    f"{part_seq}.{txt_body} => {r['receivingkey']} partno: {r['partno']}")
                                a += 1

                            for k in keys_list:
                                db.excute_data(f"update tbt_receive_datas set sync=false where receivingkey='{k}'")
                                db.excute_data(f"update tbt_receive_headers set sync=false where receive_no='{k}'")
                                
                    elif i == "ORDERPLAN":
                        whsname = "CK2"
                        # read orderplan
                        docs = y.read_ck_orderplan(fname)
                        for a in docs:
                            sql_insert_db = f"""insert into tbt_order_datas
                            (id, gedi_id,vendor, cd, unit, whs, tagrp, factory, sortg1, sortg2, sortg3, plantype, orderid, pono, recid, biac, shiptype, etdtap, partno, partname, pc, commercial, sampleflg, orderorgi, orderround, firmflg, shippedflg, shippedqty, ordermonth, balqty, bidrfl, deleteflg, ordertype, reasoncd, upddte, updtime, carriercode, bioabt, bicomd, bistdp, binewt, bigrwt, bishpc, biivpx, bisafn, biwidt, bihigh, bileng, lotno, minimum, maximum, picshelfbin, stkshelfbin, ovsshelfbin, picshelfbasicqty, outerpcs, allocateqty, sync, created_at, updated_at)
                            values(uuid_generate_v4(), '{file_id}', '{a['vendor']}', '{a['cd']}', '{a['unit']}', '{a['whs']}', '{a['tagrp']}', '{a['factory']}', '{a['sortg1']}', '{a['sortg2']}', '{a['sortg3']}', '{a['plantype']}', '{a['orderid']}', '{a['pono']}', '{a['recid']}', '{a['biac']}', '{a['shiptype']}', '{a['etdtap']}', '{a['partno']}', '{a['partname']}', '{a['pc']}', '{a['commercial']}', '{a['sampleflg']}', '{a['orderorgi']}', '{a['orderround']}', '{a['firmflg']}', '{a['shippedflg']}', '{a['shippedqty']}', '{a['ordermonth']}', '{a['balqty']}', '{a['bidrfl']}', '{a['deleteflg']}', '{a['ordertype']}', '{a['reasoncd']}', '{a['upddte']}', '{a['updtime']}', '{a['carriercode']}', '{a['bioabt']}', '{a['bicomd']}', '{a['bistdp']}', '{a['binewt']}', '{a['bigrwt']}', '{a['bishpc']}', '{a['biivpx']}', '{a['bisafn']}', '{a['biwidt']}', '{a['bihigh']}', '{a['bileng']}', '{a['lotno']}', '{a['minimum']}', '{a['maximum']}', '{a['picshelfbin']}', '{a['stkshelfbin']}', '{a['ovsshelfbin']}', '{a['picshelfbasicqty']}', '{a['outerpcs']}', '{a['allocateqty']}', true, current_timestamp, current_timestamp)"""
                            print(sql_insert_db)
                    else:
                        whsname = "RMW"
                        if i == "NARRIS":
                            fnme = fnme[len("NRRIS.32TE.SPL.ISSUENO."):]

                    target_path = f'{os.getenv("HOME")}/GEDI/{whsname}/{i}'
                    if os.path.exists(target_path) is False:
                        os.makedirs(target_path)

                    shutil.move(fname, f"{target_path}/{fnme}")
                    # list_file_dir = os.listdir(dir_name)
                    # if len(list_file_dir) <= 0:
                    #     os.removedirs(dir_name)
    except Exception as e:
        cloud.linenotify_error(str(e))
        sys.exit(0)
        pass

if __name__ == '__main__':
    __download_gedi()
    read_gedi_folder()
    SplCloud().check_folder("temp")
    sys.exit(0)
