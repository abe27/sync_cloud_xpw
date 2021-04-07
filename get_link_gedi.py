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
        docs = y.download(i.objtype, i.batchfile, i.linkfile())
        if docs != False:
            if i.objtype == "ORDERPLAN":
                filename = f'./data/{(i.objtype).lower()}/{i.batchid}.{(i.batchfile).upper()}'

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


# check data on floder

folder_target = ["receive", "orderplan"]
i = 0
while i < len(folder_target):
    # show list file on folder_target
    fname = f"./data/{folder_target[i]}"
    folder_list = os.listdir(fname)
    if len(folder_list) > 0:
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
                    # after upload remove text file
                    os.remove(txt_append)
                    print(f"remove => {r}")

            x += 1

        cloud.clear_token(token)

    i += 1


sys.exit(0)
