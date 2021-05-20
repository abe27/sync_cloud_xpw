import io
import sys
import os
import PyPDF4
from yazaki_packages.cloud import SplCloud

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

                pds_no              = None
                to_etd_date         = None
                to_etd_time         = None
                from_etd_date       = None
                from_etd_time       = None
                dest_name           = None
                name_001            = None
                comany_name         = None
                from_tap            = None
                tap_round           = None
                pl_limit            = None
                pl_no               = None
                group_no            = None
                page_no             = None
                acc_no              = None
                delivery_from_date  = None
                delivery_from_time  = None
                delivery_to_date    = None
                delivery_to_time    = None

                doc = []

                if os.path.exists(f"./temp/{i.replace('pdf', 'txt')}"):
                    os.remove(f"./temp/{i.replace('pdf', 'txt')}")

                f = open(f"./temp/{i.replace('pdf', 'txt')}", "a+")

                j = 0
                x = 0
                for r in data:
                    r = str(r).strip()
                    txt = (f"{str(str(x) + '.').ljust(5)} => {str(len(r)).ljust(5)} ::: {r}\n")
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
                        delivery_from_date  = r

                    if x == 30:
                        delivery_from_time  = r

                    if x == 31:
                        delivery_to_date    = r

                    if x == 32:
                        delivery_to_time    = r

                    if x > 67:
                        print(f"{j} ==> {r}")
                        j += 1
                        if j >= 5:
                            j = 0
                            print(f"========= end ========")
                    


                    f.write(txt)
                    x += 1

                doc.append({
                    "pds_no":pds_no,
                    "to_etd_date":to_etd_date,
                    "to_etd_time":to_etd_time,
                    "from_etd_date":from_etd_date,
                    "from_etd_time":from_etd_time,
                    "dest_name":dest_name,
                    "name_001":name_001,
                    "comany_name":comany_name,
                    "from_tap":from_tap,
                    "tap_round":tap_round,
                    "pl_limit":pl_limit,
                    "pl_no":pl_no,
                    "group_no":group_no,
                    "page_no":page_no,
                    "acc_no":acc_no,
                    "delivery_from_date":delivery_from_date,
                    "delivery_from_time":delivery_from_time,
                    "delivery_to_date":delivery_to_date,
                    "delivery_to_time":delivery_to_time,
                    "part_detail": [],
                })

                print(list(doc))

                f.close()
                

if __name__ == '__main__':
    main()
    sys.exit(0)