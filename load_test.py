from yazaki_packages.db import PsDb
import datetime

def main():
    sql = f"select id,batch_file_name,upload_date,upload_time from tbt_gedi_datas where batch_file_name like 'OES.VCB%'"
    doc = PsDb().get_fetch_all(sql)
    for i in doc:
        filename = str(i[1])
        _date = datetime.datetime.strptime(filename[17:(25+6)], "%Y%m%d%H%M%S") #OES.VCBI.32T5.SPL20210422203000.TXT
        sql_update = f"""update tbt_gedi_datas 
        set 
        download=0,
        is_download_from_cloud=true,
        upload_date='{str(_date)}',
        upload_time='{str(_date)}',
        created_at='{str(_date)}',
        updated_at='{str(_date)}',
        yazaki_id='65d0bc07-8ce4-4eed-8abd-a25bf389301b',
        user_id='2afb9037-0690-4a1b-aebe-04f17a9ba451',
        gedi_type_id='711b7af0-02f7-4437-b59b-c6d09c74da1b'
        where id='{i[0]}'"""
        PsDb().excute_data(sql_update)
        print(f"update {i[0]}")

if __name__ == '__main__':
    main()