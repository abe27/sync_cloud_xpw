# This script for get gedi form yazaki cloud.
# Then upload gedi to spl cloud.

import os
from yazaki_packages.cloud import SplCloud
from yazaki_packages.yazaki import Yk

from datetime import datetime

import pathlib
import sys

from dotenv import load_dotenv
env_path = f'{pathlib.Path().absolute()}/.env'
load_dotenv(env_path)

# initial function
y = Yk()
cloud = SplCloud()


# get yazaki link
yazaki_link = y.get_link()
if len(yazaki_link) > 0:
    for i in yazaki_link:
        docs = y.download(i.filetype, i.batchfile, i.linkfile())
        if docs != False:
            filename = f'./data/{(i.filetype).lower()}/{i.batchid}.{(i.batchfile).upper()}'

            # check duplicate file gedi. remove when exits.
            if os.path.exists(filename) == True:
                os.remove(filename)

            f = open(filename, mode='a', encoding='ascii', newline='\r\n')
            for p in docs:
                f.write(p.text)

            f.close()
        
        else:
            msg = f"Can't download {i.batchfile}!"
            cloud.linenotify(msg)

    msg = f"create gedi {len(yazaki_link)} completed."
    cloud.linenotify(msg)


# check data on floder
folder_target = ["receive", "orderplan"]
i = 0
while i < len(folder_target):
    # show list file on folder_target
    fname = f"./data/{folder_target[i]}"
    folder_list = os.listdir(fname)
    if len(folder_list) > 0:
        line_doc = []
        token = cloud.get_token()
        x = 0
        while x < len(folder_list):
            # use text file only
            r = folder_list[x]
            if r != ".gitkeep":
                # upload text file spl cloud
                txt_append = f"{fname}/{r}"
                docs = {
                    'yazaki_id': os.getenv('YAZAKI_USER'),
                    'gedi_type': (folder_target[i]).upper(),
                    'batch_id': r[:7],
                    'file_name': (r[8:]).upper(),
                    'file_path': f"{pathlib.Path().absolute()}/data/{folder_target[i]}/{r}",
                    'batch_size': os.path.getsize(txt_append),
                    'upload_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'download': 0,
                    'is_type': 'U',
                    'token': token,
                }

                if cloud.upload_gedi_to_cloud(docs):
                    line_doc.append(len(line_doc))
                    # after upload remove text file
                    os.remove(txt_append)
                    print(f"remove => {r}")

            x += 1

        cloud.clear_token(token)
        # notifies on line message
        if len(line_doc) > 0:
            msg = f"Upload {(folder_target[i]).upper()}({len(line_doc)}) to XPW Online completed."
            cloud.linenotify(msg)

        line_doc = []

    i += 1

# get download gedi
token = cloud.get_token()
doc = cloud.download_gedi(token)
if doc != False:
    if len(doc) > 0:
        r = doc[0]
        obj = r['data']
        i = 0
        while i < len(obj):
            a = obj[i]
            docs = cloud.get_text_file(f"http://{os.getenv('HOSTNAME')}{a['file_path']}")
            gedi_type = a['gedi_types']['title']
            filename = f'./temp/{(gedi_type).upper()}/{datetime.now().strftime("%Y%m%d")}'

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
            cloud.linenotify(f'download G-EDI({(a["batch_file_name"]).upper()}) completed')
            cloud.update_gedi_status(token, a['id'], 1)
            i += 1


cloud.clear_token(token)


sys.exit(0)
