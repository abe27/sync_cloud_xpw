# คำเตือน
**package** นี้มีไว้สำหรับงาน **yazaki** และ **spl** เท่านั้น

## วิธีใช้งาน
1. ทำการสร้างติดตั้ง package ด้วยคำสั่ง 
>pip install  yazaki-packages

2. สร้างไฟล์ **.env** ที่ตำแหน่งที่ต้องการทำงาน
> dir/project/.env

3. ทำการคัดลอกข้อความ **ข้างล่างนี้** ลงไปในไฟล์ **.env**
>[SPL_CLOUD]<br />
>HOSTNAME=127.0.0.1:8000<br />
>SPL_USERNAME=<br />
>SPL_PASSWORD=<br />
><br />
>[YAZAKI_CLOUD]<br />
>YAZAKI_TYPE=<br />
>YAZAKI_HOST=<br />
>YAZAKI_PORT=<br />
>YAZAKI_USER=<br />
>YAZAKI_PASSWD=<br />
>
>[DEFAULT_IS_FALSE]<br />
>YAZAKI_ANCHOR_TAG=False<br />
>
>LINE_NOTIFY_TOKEN=<br />
>[DATABASE_CONNECT_STRING]<br />>
>DB_HOST=<br />
>DB_PORT=<br />
>DB_NAME=<br />
>DB_USER=<br />
>DB_PASSWD=<br />

4. ทำการสร้างไฟล์ **.py** และเรียกใช้งาน **yazaki-packages**
>import os<br />
>from yazaki_packages.cloud import SplCloud<br />
>from yazaki_packages.yazaki import Yk<br />
><br />
>from dotenv import load_dotenv<br />
>env_path = f'{pathlib.Path().absolute()}/.env'<br />
>load_dotenv(env_path)<br />
>
>\#initial function<br />
>y = Yk()<br />
>cloud = SplCloud()<br />

**+++++++++++++++++++++++++++++++++++++++++**
<br />
>python setup.py sdist bdist_wheel
><br />
>python -m twine upload dist/*

**+++++++++++++++++++++++++++++++++++++++++**