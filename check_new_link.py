# This script for get gedi form yazaki cloud.
# Then upload gedi to spl cloud.

import os
from yazaki_packages.cloud import SplCloud
from yazaki_packages.yazaki import Yk
from yazaki_packages.db import PsDb

from datetime import datetime
from termcolor import colored

import pathlib
import sys
import time

from dotenv import load_dotenv
app_path = f'{pathlib.Path().absolute()}'
env_path = f"{app_path}/.env"
load_dotenv(env_path)

print(colored(
    f"========== start download from yazaki at: {datetime.now()} ==========", "yellow"))

# initial function
y = Yk()
cloud = SplCloud()

def main(yazaki_link):
    if len(yazaki_link) > 0:
        for i in yazaki_link:
            docs = y.download(i.filetype, i.batchfile, i.linkfile())
            if docs != False:
                filename = f'{app_path}/data/{(i.filetype).lower()}/{i.batchid}.{(i.batchfile).upper()}'

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

        # msg = f"create gedi {len(yazaki_link)} completed."
        # cloud.linenotify(msg)

    print(colored(
        f"============ end download from yazaki at: {datetime.now()} ===================", "yellow"))

    print("\n")
    print(colored("================= begin start upload to spl cloud ==================", "green"))

def __get_link_yazaki():
    # get yazaki link
    main(y.get_link())
    main(y.get_link_centrol())
    

def __upload_to_spl_cloud():
    # check data on floder
    folder_target = cloud.check_folder("data")
    i = 0
    while i < len(folder_target):
        # show list file on folder_target
        fname = f"{app_path}/data/{folder_target[i]}"
        folder_list = os.listdir(fname)
        if len(folder_list) > 0:
            line_doc = []
            token = cloud.get_token()
            if token != False:
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
                            'file_path': f"{app_path}/data/{folder_target[i]}/{r}",
                            'batch_size': os.path.getsize(txt_append),
                            'upload_date': datetime.now().strftime('%Y-%m-%d %X'),
                            'download': 0,
                            'is_type': 'U',
                            'token': token,
                        }
                        
                        if cloud.upload_gedi_to_cloud(docs):
                            line_doc.append(len(line_doc))
                            # after upload remove text file
                            os.remove(txt_append)
                            print(
                                colored(f"update data to spl cloud => {r}", "blue"))

                    x += 1

                # notifies on line message
                if len(line_doc) > 0:
                    msg = f"upload {(folder_target[i]).upper()}({len(line_doc)}) to XPW Online completed."
                    cloud.linenotify(msg)

            line_doc = []

        i += 1



if __name__ == '__main__':
    check_reload = PsDb().get_fetch_one(f"select id from tbt_refresh_gedis where refresh_status=false")
    if check_reload != False:
        __get_link_yazaki()
        __upload_to_spl_cloud()

    PsDb().excute_data(f"update tbt_refresh_gedis set refresh_status=true")
    cloud.check_folder("data")
    sys.exit(0)
    





