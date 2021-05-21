import io
import sys
import os
import PyPDF4
import datetime
import shutil
from yazaki_packages.cloud import SplCloud
from yazaki_packages.db import OraFG


def insert_db(obj):
    i = 1
    for r in obj:
        arrival_from_date = f"null"
        if len(r['arrival_from_date']) > 0:
            arrival_from_date = f"to_date('{r['arrival_from_date']}','DD/MM/YY')"

        departure_date = f"null"
        if len(r['departure_date']) > 0:
            departure_date = f"to_date('{r['departure_date']}', 'DD/MM/YY')"


        sql = f"""INSERT INTO TMP_ORDER(ITEM, ARRIVAL_DATE, ARRIVAL_TIME, COLLECT_DATE, COLLECT_TIME, MAIN_ROUTE, DOCNO, SUPPLIER_NAME, SUPPLIER_CODE, ORDERNO, DOCKCODE, LAST_MAIN_ROUTE, LAST_, MIROSNO, SUPPLIER_PICKUP, TO_ARRIVAL_DATE, TO_ARRIVAL_TIME, DEPARTURE_DATE, DEPARTURE_TIME, PARTNO, CHANGENO, KANBANNO, ADDRESS, PACKQTY, ORDER_KBCT, QTY, FILE_NAME, SYSDTE, SYNC)
        VALUES('{i}',to_date('{r['arrival_date']}', 'DD/MM/YYYY'),'{r['arrival_time']}',to_date('{r['collect_date']}','DD/MM/YYYY'),'{r['collect_time']}','{r['main_route_no']}','{r['pds_no']}','{r['supplier_name']}','{r['supplier_code']}','{r['order_no']}','{r['dock_code']}','{r['main_route_no']}','{r['last_no']}','{r['mros_no']}','{r['supp_pick_no']}',{arrival_from_date},'{r['arrival_from_time']}',{departure_date},'{r['departure_time']}','{r['part_no']}','{r['part_change']}','{r['kanban_no']}','{r['addess']}','{r['part_stdpack']}','{r['part_ctn']}','{r['part_qty']}','{r['file_name']}',current_timestamp, 0)"""
        
        if OraFG().get_fetch_one(f"SELECT ITEM FROM TMP_ORDER WHERE DOCNO='{r['pds_no']}' and ORDERNO='{r['order_no']}' and PARTNO='{r['part_no']}'") > 0:
            sql = f"""UPDATE TMP_ORDER SET ITEM={i},ORDER_KBCT='{r['part_ctn']}', QTY='{r['part_qty']}', SYNC=0 WHERE DOCNO='{r['pds_no']}' and ORDERNO='{r['order_no']}' and PARTNO='{r['part_no']}'"""
        
        OraFG().excute_data(sql)
        i += 1
        
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

                txt_name = f"./temp/pages-{j}-{i.replace('pdf', 'txt')}"
                # if os.path.exists(txt_name):
                #     os.remove(txt_name)

                # f = open(txt_name, "a+")

                num_line = 67
                p = 0
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
                            if len(r) == 14:
                                num_line = 32

                            else:
                                num_line = 65

                    # fixed_part = True
                    # if len(r) == 18:
                    #     fixed_part = False

                    if x > num_line:
                        if r.find('TOTAL') < 0:
                            if p == 0:
                                part_no = r
                                # if len(r) >= 14:
                                #     part_no = r[:11]
                                #     part_change = r[12:]
                                if len(r) >= 18:
                                    part_no = r[:14]
                                    part_change = r[15:]

                            if p == 1:
                                part_tag_code = r

                            if p == 2:
                                part_tag_name = r

                            if p == 3:
                                part_stdpack = r

                            if p == 4:
                                part_ctn = r

                            if p == 5:
                                part_qty = r

                            p += 1
                            if p >= 6:
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
                                    "pages": j,
                                    "filename": txt_name
                                })
                                p = 0
                                part_no = ""
                                part_change = ""
                                part_tag_code = ""
                                part_tag_name = ""
                                part_stdpack = ""
                                part_ctn = ""
                                part_qty = ""

                    # f.write(txt)
                    # print(txt)
                    x += 1

                # f.close()
                print(f"========= {txt_name} ==> {i} ::: PAGE => {j} ========")
                # print(doc)
                insert_db(doc)

            pdfFileObject.close()
            move_to_home(f"./pdf/{i}", i)


if __name__ == '__main__':
    main()
    sys.exit(0)
