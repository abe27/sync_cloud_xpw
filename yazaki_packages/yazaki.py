class ObjectLink(object):
    def __init__(self, objtype, mailbox, batchid, size, batchfile, currentdate, flags, formats, orgname, download=False):
        import os
        from datetime import datetime
        ordn = None
        bf = 0
        filetype = "RECEIVE"
        factory = "INJ"

        if objtype == 'RMW':
            ordn = str(batchfile).strip()
            factory = "RMW"
            filename = ""
            if ordn[:3] == "OES":
                filename = ordn[len("OES.32TE.SPL."):]
            else:
                filename = ordn[len("NRRIS.32TE.SPL."):]

            filename = filename[:filename.find(".")].upper()
            if filename == "ISSUELIST":
                filetype = "CONLOT"

            elif filename == "ISSUENO":
                filetype = "KANBAN"

            else:
                filetype = "RECEIVE"

        elif objtype == 'CK2':
            ordn = str(batchfile[:len("OES.VCBI")]).strip()
            bf = int(str(batchfile[len("OES.VCBI") + 3:])[1:2].strip())
            filetype = "RECEIVE"
            if ordn == "OES.VCBI":
                filetype = "ORDERPLAN"

            factory = "INJ"
            if bf == 4:
                factory = "AW"

        elif objtype == 'J03':
            print("J03")

        elif objtype == 'FG':
            print("FG")

        else:
            print("UNKNOW")

        self.objtype = objtype
        self.mailbox = mailbox
        self.batchid = batchid
        self.size = size
        self.batchfile = batchfile
        self.currentdate = datetime.strptime(currentdate, '%b %d, %Y %I:%M %p')
        self.flags = flags
        self.formats = formats
        self.orgname = orgname
        self.factory = factory
        self.filetype = filetype
        self.download = download

    def linkfile(self):
        import os
        return f"https://{os.getenv('YAZAKI_HOST')}:{os.getenv('YAZAKI_PORT')}/cehttp/servlet/MailboxServlet?operation=DOWNLOAD&mailbox_id={self.mailbox}&batch_num={self.batchid}&data_format=A&batch_id={self.batchfile}"


class Yk:
    def __trimtxt(self, txt):
        return str(txt).lstrip().rstrip()

    def __restrip(self, txt):
        return (txt).rstrip().lstrip()

    def __repartname(self, txt):
        return (str(txt).replace("b", "")).replace("'", "")

    def __returnutfpono(self, txt):
        return str(self.__repartname(txt)).strip()

    def __checknamepart(self, fac, part):
        p = str(part).lstrip().rstrip().replace('.', '')
        partname = p
        if fac == "AW":
            try:
                k = str(p[: p.index(" ")]).strip()
                s = p[len(k):]
                ss = s.strip()
                sn = str(ss[:ss.index(" ")]).strip()
                ssize = str(ss[:ss.index(" ")])

                if len(sn) > 1:
                    ssize = str(f"{sn[:1]}.{sn[1:]}").strip()

                c = str(p[(len(k) + len(ssize)) + 1:]).strip()
                partname = f"{k} {ssize} {c}"
            except:
                pass
            finally:
                pass

        return partname

    def read_ck_receive(self, filename):
        from datetime import datetime
        import uuid

        f = open(filename, 'r', encoding='utf-8')
        docs = []
        for i in f:
            fac = filename[filename.find('SPL') - 2:filename.find('SPL') - 1]
            uuidcode = str(uuid.uuid4())
            plantype = "RECEIVE"
            cd = 20
            unit = 'BOX'
            recisstype = '01'
            factory = 'INJ'
            if fac != "5":
                factory = 'AW'
                plantype = "RECEIVE"
                cd = 10
                unit = 'COIL'
                recisstype = '01'

            line = i
            try:
                docs.append({
                    'factory': factory,
                    "faczone": str(self.__trimtxt(line[4:(4 + 3)])),
                    "receivingkey": str(self.__trimtxt(line[4:(4 + 12)])),
                    "partno": str(self.__trimtxt(line[76:(76 + 25)])),
                    "partname": str(self.__trimtxt(line[101:(101 + 25)])),
                    'vendor': factory,
                    'cd': cd,
                    'unit': unit,
                    'whs': factory,
                    'tagrp': 'C',
                    "recisstype": recisstype,
                    "plantype": plantype,
                    "recid": str(self.__trimtxt(line[0:4])),
                    "aetono": str(self.__trimtxt(line[4:(4 + 12)])),
                    "aetodt": str(self.__trimtxt(line[16:(16 + 10)])),
                    "aetctn": float(str(self.__trimtxt(line[26:(26 + 9)]))),
                    "aetfob": float(str(self.__trimtxt(line[35:(35 + 9)]))),
                    "aenewt": float(str(self.__trimtxt(line[44:(44 + 11)]))),
                    "aentun": str(self.__trimtxt(line[55:(55 + 5)])),
                    "aegrwt": float(str(self.__trimtxt(line[60:(60 + 11)]))),
                    "aegwun": str(self.__trimtxt(line[71:(71 + 5)])),
                    "aeypat": str(self.__trimtxt(line[76:(76 + 25)])),
                    "aeedes": str(self.__checknamepart(factory, self.__repartname(line[101:(101 + 25)]))),
                    "aetdes": str(self.__checknamepart(factory, self.__repartname(line[101:(101 + 25)]))),
                    "aetarf": float(str(self.__trimtxt(line[151:(151 + 10)]))),
                    "aestat": float(str(self.__trimtxt(line[161:(161 + 10)]))),
                    "aebrnd": float(str(self.__trimtxt(line[171:(171 + 10)]))),
                    "aertnt": float(str(self.__trimtxt(line[181:(181 + 5)]))),
                    "aetrty": float(str(self.__trimtxt(line[186:(186 + 5)]))),
                    "aesppm": float(str(self.__trimtxt(line[191:(191 + 5)]))),
                    "aeqty1": float(str(self.__trimtxt(line[196:(196 + 9)]))),
                    "aeqty2": float(str(self.__trimtxt(line[205:(205 + 9)]))),
                    "aeuntp": float(str(self.__trimtxt(line[214:(214 + 9)]))),
                    "aeamot": float(str(self.__trimtxt(line[223:(223 + 11)]))),
                    "plnctn": float(str(self.__trimtxt(line[26:(26 + 9)]))),
                    "plnqty": float(str(self.__trimtxt(line[196:(196 + 9)]))),
                    "minimum": 0,
                    "maximum": 0,
                    "picshelfbin": "PNON",
                    "stkshelfbin": "SNON",
                    "ovsshelfbin": "ONON",
                    "picshelfbasicqty": 0,
                    "outerpcs": 0,
                    "allocateqty": 0,
                    "sync": False,
                    "uuid": uuidcode,
                    "updatedon": datetime.now()
                })
            except Exception as ex:
                print(ex)
                pass
        return docs

    def read_ck_orderplan(self, filename):
        from datetime import datetime
        import uuid

        f = open(filename, 'r', encoding='utf-8')
        docs = []
        for line in f:
            fac = filename[filename.find('SPL') - 2:filename.find('SPL') - 1]
            uuidcode = str(uuid.uuid4())
            plantype = "ORDERPLAN"
            cd = 20
            unit = 'BOX'
            sortg1 = 'PARTTYPE'
            sortg2 = 'PARTNO'
            sortg3 = ''
            factory = "INJ"

            if fac != '5':
                factory = "AW"
                plantype = "ORDERPLAN"
                cd = 10
                unit = 'COIL'
                sortg1 = 'PONO'
                sortg2 = 'PARTTYPE'
                sortg3 = 'PARTNO'

            oqty = str(self.__trimtxt(line[89:(89 + 9)]))
            if oqty == "":
                oqty = 0

            try:
                docs.append({
                    'vendor': factory,
                    'cd': cd,
                    'unit': unit,
                    'whs': factory,
                    'tagrp': 'C',
                    'factory': factory,
                    "sortg1": sortg1,
                    "sortg2": sortg2,
                    "sortg3": sortg3,
                    "plantype": plantype,
                    "orderid": str(self.__trimtxt(line[13:(13 + 15)])),
                    # remove space
                    "pono": str(self.__returnutfpono(line[13:(13 + 15)])),
                    "recid": str(self.__trimtxt(line[0:4])),
                    "biac": str(self.__trimtxt(line[5:(5 + 8)])),
                    "shiptype": str(self.__trimtxt(line[4:(4 + 1)])),
                    "etdtap": datetime.strptime(str(self.__trimtxt(line[28:(28 + 8)])), '%Y%m%d'),
                    "partno": str(self.__trimtxt(line[36:(36 + 25)])),
                    "partname": str(self.__checknamepart(factory, self.__returnutfpono(line[61:(61 + 25)]))),
                    "pc": str(self.__trimtxt(line[86:(86 + 1)])),
                    "commercial": str(self.__trimtxt(line[87:(87 + 1)])),
                    "sampleflg": str(self.__trimtxt(line[88:(88 + 1)])),
                    "orderorgi": int(oqty),
                    "orderround": int(str(self.__trimtxt(line[98:(98 + 9)]))),
                    "firmflg": str(self.__trimtxt(line[107:(107 + 1)])),
                    "shippedflg": str(self.__trimtxt(line[108:(108 + 1)])),
                    "shippedqty": float(str(self.__trimtxt(line[109:(109 + 9)]))),
                    "ordermonth": datetime.strptime(str(self.__trimtxt(line[118:(118 + 8)])), '%Y%m%d'),
                    "balqty": float(str(self.__trimtxt(line[126:(126 + 9)]))),
                    "bidrfl": str(self.__trimtxt(line[135:(135 + 1)])),
                    "deleteflg": str(self.__trimtxt(line[136:(136 + 1)])),
                    "ordertype": str(self.__trimtxt(line[137:(137 + 1)])),
                    "reasoncd": str(self.__trimtxt(line[138:(138 + 3)])),
                    "upddte": datetime.strptime(str(self.__trimtxt(line[141:(141 + 14)])), '%Y%m%d%H%M%S'),
                    "updtime": datetime.strptime(str(self.__trimtxt(line[141:(141 + 14)])), '%Y%m%d%H%M%S'),
                    "carriercode": str(self.__trimtxt(line[155:(155 + 4)])),
                    "bioabt": int(str(self.__trimtxt(line[159:(159 + 1)]))),
                    "bicomd": str(self.__trimtxt(line[160:(160 + 1)])),
                    "bistdp": float(str(self.__trimtxt(line[165:(165 + 9)]))),
                    "binewt": float(str(self.__trimtxt(line[174:(174 + 9)]))),
                    "bigrwt": float(str(self.__trimtxt(line[183:(183 + 9)]))),
                    "bishpc": str(self.__trimtxt(line[192:(192 + 8)])),
                    "biivpx": str(self.__trimtxt(line[200:(200 + 2)])),
                    "bisafn": str(self.__trimtxt(line[202:(202 + 6)])),
                    "biwidt": float(str(self.__trimtxt(line[212:(212 + 4)]))),
                    "bihigh": float(str(self.__trimtxt(line[216:(216 + 4)]))),
                    "bileng": float(str(self.__trimtxt(line[208:(208 + 4)]))),
                    "lotno": str(self.__trimtxt(line[220:(220 + 8)])),
                    "minimum": 0,
                    "maximum": 0,
                    "picshelfbin": "PNON",
                    "stkshelfbin": "SNON",
                    "ovsshelfbin": "ONON",
                    "picshelfbasicqty": 0,
                    "outerpcs": 0,
                    "allocateqty": 0,
                    "sync": False,
                    "uuid": uuidcode,
                    "updatedon": datetime.strptime(str(self.__trimtxt(line[141:(141 + 14)])), '%Y%m%d%H%M%S')
                })
            except Exception as ex:
                print(ex)
                pass

        # print(f"found orderplan: {len(docs)}")
        return docs

    def __login(self):
        import sys
        import os
        import urllib
        import urllib3
        import requests
        from yazaki_packages.logs import Logging

        resp = False
        try:
            # login yazaki website.
            url = f"https://{os.getenv('YAZAKI_HOST')}:{os.getenv('YAZAKI_PORT')}/cehttp/servlet/MailboxServlet"
            passwd = urllib.parse.quote(os.getenv('YAZAKI_PASSWD'))
            payload = f"operation=LOGON&remote={os.getenv('YAZAKI_USER')}&password={passwd}"
            headers = {'Content-Type': "application/x-www-form-urlencoded"}
            urllib3.disable_warnings()
            resp = requests.request(
                "POST", url, data=payload, headers=headers, verify=False, timeout=3)
            print(f"{os.getenv('YAZAKI_USER')} => login success")
            Logging(os.getenv('YAZAKI_USER') , f"{os.getenv('YAZAKI_USER')} login", "success")

        except Exception as msg:
            Logging(os.getenv('YAZAKI_USER') , f"{os.getenv('YAZAKI_USER')} login", 'error: ' + str(msg))
            sys.exit(0)

        return resp

    def __login_central(self):
        import sys
        import os
        import urllib
        import urllib3
        import requests
        from yazaki_packages.logs import Logging
        resp = False
        try:
            # login yazaki website.
            url = f"https://{os.getenv('YAZAKI_HOST')}:{os.getenv('YAZAKI_PORT')}/cehttp/servlet/MailboxServlet"
            passwd = urllib.parse.quote(os.getenv('WHS_YAZAKI_PASSWD'))
            payload = f"operation=LOGON&remote={os.getenv('WHS_YAZAKI_USER')}&password={passwd}"
            headers = {'Content-Type': "application/x-www-form-urlencoded"}
            urllib3.disable_warnings()
            resp = requests.request(
                "POST", url, data=payload, headers=headers, verify=False, timeout=3)
            print(f"{os.getenv('WHS_YAZAKI_USER')} => login success")
            Logging({os.getenv('WHS_YAZAKI_USER')} , f"{os.getenv('WHS_YAZAKI_USER')} login", "success")

        except Exception as msg:
            Logging({os.getenv('WHS_YAZAKI_USER')} , f"{os.getenv('WHS_YAZAKI_USER')} login", 'error: ' + str(msg))
            sys.exit(0)

        return resp

    def __logout_central(self, session):
        import requests
        import os
        from yazaki_packages.logs import Logging

        url = f"https://{os.getenv('YAZAKI_HOST')}:{os.getenv('YAZAKI_PORT')}/cehttp/servlet/MailboxServlet?operation=LOGOFF"
        headers = {}
        pyload = {}
        requests.request("POST", url, data=pyload, headers=headers,
                             verify=False, timeout=3, cookies=session.cookies)
        print(f"{os.getenv('WHS_YAZAKI_USER')} => logout success")
        Logging(os.getenv('WHS_YAZAKI_USER') , f"{os.getenv('WHS_YAZAKI_USER')} logout", "success")
        return True

    def __logout(self, session):
        import requests
        import os
        from yazaki_packages.logs import Logging

        url = f"https://{os.getenv('YAZAKI_HOST')}:{os.getenv('YAZAKI_PORT')}/cehttp/servlet/MailboxServlet?operation=LOGOFF"
        headers = {}
        pyload = {}
        requests.request("POST", url, data=pyload, headers=headers,
                             verify=False, timeout=3, cookies=session.cookies)
        print(f"{os.getenv('YAZAKI_USER')} => logout success")
        Logging(os.getenv('YAZAKI_USER') , f"{os.getenv('YAZAKI_USER')} logout", "success")
        return True

    def get_link_central(self):
        obj = []
        try:
            import datetime
            from datetime import timedelta
            import requests
            from bs4 import BeautifulSoup
            from termcolor import colored
            from yazaki_packages.logs import Logging
            import os

            etd = str((datetime.datetime.now() - timedelta(days=7)).strftime('%Y%m%d'))

            # get cookies after login.
            session = self.__login_central()
            if session.status_code == 200:
                # get html page
                url = f"https://{os.getenv('YAZAKI_HOST')}:{os.getenv('YAZAKI_PORT')}/cehttp/servlet/MailboxServlet"
                headers = {'Content-Type': "application/x-www-form-urlencoded"}
                pyload = f"operation=DIRECTORY&fromdate={etd}&Submit=Receive"
                r = requests.request("POST", url, data=pyload, headers=headers,
                                     verify=False, timeout=3, cookies=session.cookies)
                # print(type(r))
                soup = BeautifulSoup(r.text, 'html.parser')
                for tr in soup.find_all('tr'):
                    found = False
                    i = 0
                    docs = []
                    for td in tr.find_all('td'):
                        txt = self.__restrip(td.text)
                        docs.append(txt)
                        if td.find("a") != None:
                            found = True

                        if found is True:
                            if len(docs) >= 9:
                                l = ObjectLink(os.getenv('WHS_YAZAKI_TYPE'), docs[0], docs[1], str(docs[2]).replace(
                                    ",", "").strip(), docs[3], f"{docs[4]} {docs[5]}", docs[6], docs[7], docs[8], found)
                                obj.append(l)

                        i += 1

                # logout
                if len(obj) > 0:
                    print(colored(f"found new link => {len(obj)}", "green"))
                    Logging(os.getenv('WHS_YAZAKI_USER') , f"{os.getenv('WHS_YAZAKI_USER')} get data {len(obj)}", "success")

                self.__logout_central(session)

        except Exception as ex:
            Logging(os.getenv('WHS_YAZAKI_USER') , f"{os.getenv('WHS_YAZAKI_USER')} get data", 'error: ' + str(ex))
            pass

        return obj

    def get_link(self):
        obj = []
        try:
            import datetime
            from datetime import timedelta
            import requests
            from bs4 import BeautifulSoup
            from termcolor import colored
            from yazaki_packages.logs import Logging
            import os

            etd = str((datetime.datetime.now() - timedelta(days=7)).strftime('%Y%m%d'))

            # get cookies after login.
            session = self.__login()
            if session.status_code == 200:
                # get html page
                url = f"https://{os.getenv('YAZAKI_HOST')}:{os.getenv('YAZAKI_PORT')}/cehttp/servlet/MailboxServlet"
                headers = {'Content-Type': "application/x-www-form-urlencoded"}
                pyload = f"operation=DIRECTORY&fromdate={etd}&Submit=Receive"
                r = requests.request("POST", url, data=pyload, headers=headers,
                                     verify=False, timeout=3, cookies=session.cookies)
                # print(type(r))
                soup = BeautifulSoup(r.text, 'html.parser')
                for tr in soup.find_all('tr'):
                    found = False
                    i = 0
                    docs = []
                    for td in tr.find_all('td'):
                        txt = self.__restrip(td.text)
                        docs.append(txt)
                        if td.find("a") != None:
                            found = True

                        if found is True:
                            if len(docs) >= 9:
                                l = ObjectLink(os.getenv("YAZAKI_TYPE"), docs[0], docs[1], str(docs[2]).replace(
                                    ",", "").strip(), docs[3], f"{docs[4]} {docs[5]}", docs[6], docs[7], docs[8], found)
                                obj.append(l)

                        i += 1

                # logout
                if len(obj) > 0:
                    print(colored(f"found new link => {len(obj)}", "green"))
                    Logging(os.getenv('YAZAKI_USER') , f"{os.getenv('YAZAKI_USER')} get data {len(obj)}", "success")

                self.__logout(session)

        except Exception as ex:
            Logging(os.getenv('YAZAKI_USER') , f"{os.getenv('YAZAKI_USER')} get data", 'error: ' + str(ex))
            pass

        return obj

    def download_central(self, currentdate, objtype, filename, filelink):
        from datetime import datetime
        import requests
        from bs4 import BeautifulSoup
        from termcolor import colored
        from yazaki_packages.logs import Logging
        import os

        session = self.__login_central()

        docs = False
        try:
            if session is not None:
                if session.status_code == 200:
                    # download file
                    rq = requests.get(filelink, stream=True, verify=False,
                                      cookies=session.cookies, allow_redirects=True)
                    docs = BeautifulSoup(rq.content, 'lxml')
                    print(colored(f"download gedi {objtype} file : {(filename).upper()}", "blue"))
                    Logging(os.getenv('WHS_YAZAKI_USER') , f"download {objtype} file : {(filename).upper()} upload_at: {str(currentdate)}", "success")
                    # logout
                    self.__logout_central(session)

        except Exception as ex:
            Logging(os.getenv('WHS_YAZAKI_USER') , f"download gedi {objtype} file : {(filename).upper()}", 'error: ' + str(ex))
            pass

        return docs

    def download(self, currentdate, objtype, filename, filelink):
        from datetime import datetime
        import requests
        from bs4 import BeautifulSoup
        from termcolor import colored
        from yazaki_packages.logs import Logging
        import os

        session = self.__login()

        docs = False
        try:
            if session is not None:
                if session.status_code == 200:
                    # download file
                    rq = requests.get(filelink, stream=True, verify=False,
                                      cookies=session.cookies, allow_redirects=True)
                    docs = BeautifulSoup(rq.content, 'lxml')
                    print(colored(f"download gedi {objtype} file : {(filename).upper()}", "blue"))
                    Logging(os.getenv('YAZAKI_USER') , f"download {objtype} file : {(filename).upper()} upload_at: {str(currentdate)}", "success")
                    # logout
                    self.__logout(session)

        except Exception as ex:
            Logging(os.getenv('YAZAKI_USER') , f"download {objtype} file : {(filename).upper()}", 'error: ' + str(ex))
            pass

        return docs

    def __init__(self):
        import datetime
        from termcolor import colored
        print(colored("Yazaki Package Running At: " + str(datetime.datetime.now()), "magenta"))
