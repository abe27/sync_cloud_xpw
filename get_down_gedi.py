from yazaki_packages.cloud import SplCloud
from termcolor import colored
from datetime import datetime
import os
import pathlib

from dotenv import load_dotenv
env_path = f'{pathlib.Path().absolute()}/.env'
load_dotenv(env_path)

cloud = SplCloud()
# get download gedi
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
                    f"http://{os.getenv('HOSTNAME')}{a['file_path']}")
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
                print(colored(
                    f'download G-EDI({(a["batch_file_name"]).upper()}) completed', "yellow"))
                cloud.linenotify(
                    f'download G-EDI({(a["batch_file_name"]).upper()}) completed')
                cloud.update_gedi_status(token, a['id'], 1)
                i += 1

    cloud.clear_token(token)

print(colored(
    "================= end upload to spl cloud at {datetime.now()} ==================", "green"))
