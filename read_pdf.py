import io
import sys
import os
import PyPDF4
import datetime
import shutil
from yazaki_packages.cloud import SplCloud
from yazaki_packages.db import OraFG


def insert_db(obj):
    for r in obj:
        sql = f"""INSERT INTO TMP_ORDER(ITEM, PDS_NO, FROM_ETD_DATE, FROM_ETD_TIME, TO_ETD_DATE, TO_ETD_TIME, DEST_NAME, NAME_001, COMPANY_NAME, FROM_TAP, TAP_ROUND, PL_LIMIT, PL_NO, GROUP_NO, PAGE_NO, ACC_NO, DELIVERY_FROM_DATE, DELIVERY_FROM_TIME, DELIVERY_TO_DATE, DELIVERY_TO_TIME, PARTNO, TAG_CODE, TAG_NAME, STDPACK, CT, QTY,FILE_NAME,CHANGENO)
        VALUES({r['no']}, '{r['pds_no']}',to_date('{r['to_etd_date']}', 'DD/MM/YYYY'),'{r['to_etd_time']}',to_date('{r['from_etd_date']}', 'DD/MM/YYYY'),'{r['from_etd_time']}','{r['dest_name']}','{r['name_001']}','{r['comany_name']}','{r['from_tap']}','{r['tap_round']}','{r['pl_limit']}','{r['pl_no']}','{r['group_no']}','{r['page_no']}','{r['acc_no']}',to_date('{r['delivery_from_date']}', 'DD/MM/YY'),'{r['delivery_from_time']}',to_date('{r['delivery_to_date']}', 'DD/MM/YY'),'{r['delivery_to_time']}','{r['part_no']}','{r['part_tag_code']}','{r['part_tag_name']}','{r['part_stdpack']}','{r['part_ctn']}','{r['part_qty']}','{r['file_name']}','{r['part_change']}')"""
        if OraFG().get_fetch_one(f"select ITEM from TMP_ORDER where PDS_NO='{r['pds_no']}' and PARTNO='{r['part_no']}'") > 0:
            sql = f"""UPDATE TMP_ORDER SET CT='{r['part_ctn']}', QTY='{r['part_qty']}', SYNC=0 WHERE PDS_NO='{r['pds_no']}' and PARTNO='{r['part_no']}'"""

        OraFG().excute_data(sql)
    return


def move_to_home(source_file, filename):
    fname = os.getenv("HOME") + \
        f"/GEDI/PDF/{datetime.datetime.now().strftime('%Y%m%d')}"
    if os.path.exists(fname) is False:
        os.makedirs(fname)

    shutil.move(source_file, fname + "/"+filename)
    msg = f"UPLOAD {filename} COMPLETE."
    # SplCloud().linenotify(msg)


def main():
    fname = os.listdir("./pdf")
    for i in fname:
        if i != ".gitkeep":
            pdfFileObject = open(f"./pdf/{i}", 'rb')
            pdfReader = PyPDF4.PdfFileReader(pdfFileObject)

            count = pdfReader.numPages
            for j in range(count):
                page = pdfReader.getPage(j)
                data = io.StringIO(page.extractText())

                pds_no = ""
                to_etd_date = ""
                to_etd_time = ""
                from_etd_date = ""
                from_etd_time = ""
                dest_name = ""
                name_001 = ""
                comany_name = ""
                from_tap = ""
                tap_round = ""
                pl_limit = ""
                pl_no = ""
                group_no = ""
                page_no = ""
                acc_no = ""
                delivery_from_date = ""
                delivery_from_time = ""
                delivery_to_date = ""
                delivery_to_time = ""

                part_no = ""
                part_change = ""
                part_tag_code = ""
                part_tag_name = ""
                part_stdpack = ""
                part_ctn = ""
                part_qty = ""

                doc = []

                # if os.path.exists(f"./temp/{i.replace('pdf', 'txt')}"):
                #     os.remove(f"./temp/{i.replace('pdf', 'txt')}")

                # f = open(f"./temp/{i.replace('pdf', 'txt')}", "a+")

                num_line = 67
                j = 0
                x = 0
                for r in data:
                    r = str(r).strip()
                    txt = (
                        f"{str(str(x) + '.').ljust(5)} => {str(len(r)).ljust(5)} ::: {r}\n")
                    if x == 0:
                        pds_no = r

                    if x == 1:
                        to_etd_date = r

                    if x == 2:
                        to_etd_time = r

                    if x == 3:
                        from_etd_date = r

                    if x == 4:
                        from_etd_time = r

                    if x == 5:
                        dest_name = r

                    if x == 6:
                        name_001 = r

                    if x == 8:
                        comany_name = r

                    if x == 9:
                        from_tap = r

                    if x == 10:
                        tap_round = r

                    if x == 11:
                        pl_limit = r

                    if x == 12:
                        pl_no = r

                    if x == 17:
                        group_no = r

                    if x == 19:
                        page_no = r

                    if x == 20:
                        acc_no = r

                    if x == 29:
                        delivery_from_date = r

                    if x == 30:
                        delivery_from_time = r

                    if x == 31:
                        delivery_to_date = r

                    if x == 32:
                        delivery_to_time = r

                    if x == 33:
                        if r.find("REPRINT") < 0:
                            num_line = 65

                    # fixed_part = True
                    # if len(r) == 18:
                    #     fixed_part = False

                    if x > num_line:
                        if r.find('TOTAL') < 0:
                            if j == 0:
                                part_no = r
                                if len(r) == 18:
                                    part_no = r[:14]
                                    part_change = r[15:]

                            if j == 1:
                                part_tag_code = r

                            if j == 2:
                                part_tag_name = r

                            if j == 3:
                                part_stdpack = r

                            if j == 4:
                                part_ctn = r

                            if j == 5:
                                part_qty = r

                            j += 1
                            if j >= 6:
                                doc.append({
                                    "no": len(doc) + 1,
                                    "pds_no": pds_no,
                                    "arrival_date": to_etd_date,
                                    "arrival_time": to_etd_time,
                                    "collect_date": from_etd_date,
                                    "collect_time": from_etd_time,
                                    "dest_name": dest_name,
                                    "main_route_no": name_001,
                                    "supplier_name": comany_name,
                                    "supplier_code": from_tap,
                                    "order_no": tap_round,
                                    "page_no": pl_limit,
                                    "dock_code": pl_no,
                                    "last_no": group_no,
                                    "mros_no": page_no,
                                    "supp_pick_no": acc_no,
                                    "arrival_from_date": delivery_from_date,
                                    "arrival_from_time": delivery_from_time,
                                    "departure_date": delivery_to_date,
                                    "departure_time": delivery_to_time,
                                    "part_no": part_no,
                                    "part_change": part_change,
                                    "kanban_no": part_tag_code,
                                    "addess": part_tag_name,
                                    "part_stdpack": part_stdpack,
                                    "part_ctn": part_ctn,
                                    "part_qty": part_qty,
                                    "file_name": i,
                                })
                                j = 0
                                part_no = ""
                                part_change = ""
                                part_tag_code = ""
                                part_tag_name = ""
                                part_stdpack = ""
                                part_ctn = ""
                                part_qty = ""

                    # f.write(txt)
                    print(txt)
                    x += 1

                # f.close()
                print(f"========= {i} ========")
                insert_db(doc)

            pdfFileObject.close()
            move_to_home(f"./pdf/{i}", i)


if __name__ == '__main__':
    main()
    sys.exit(0)
