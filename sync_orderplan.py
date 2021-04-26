from yazaki_packages.db import PsDb, OraDB
from yazaki_packages.cloud import SplCloud

def main():
    sql = f"""select id,gedi_id,seq,vendor,cd,unit,whs,tagrp,factory,sortg1,sortg2,sortg3,plantype,orderid,pono,recid,biac,shiptype,etdtap,partno,partname,pc,commercial,sampleflg,orderorgi,orderround,firmflg,shippedflg,shippedqty,ordermonth,balqty,bidrfl,deleteflg,ordertype,reasoncd,upddte,updtime,carriercode,bioabt,bicomd,bistdp,binewt,bigrwt,bishpc,biivpx,bisafn,biwidt,bihigh,bileng,lotno,minimum,maximum,picshelfbin,stkshelfbin,ovsshelfbin,picshelfbasicqty,outerpcs,allocateqty,sync,created_at,updated_at 
        from tbt_order_datas 
        where sync=false
        order by created_at,seq,gedi_id"""
    docs = PsDb().get_fetch_all(sql)
    i = 0
    while i < len(docs):
        r = docs[i]
        order_id            = r[0]
        gedi_id             = r[1]
        seq                 = r[2]
        vendor              = r[3]
        cd                  = r[4]
        unit                = r[5]
        whs                 = r[6]
        tagrp               = r[7]
        factory             = r[8]
        sortg1              = r[9]
        sortg2              = r[10]
        sortg3              = r[11]
        plantype            = r[12]
        orderid             = r[13]
        pono                = r[14]
        recid               = r[15]
        biac                = r[16]
        shiptype            = r[17]
        etdtap              = r[18]
        partno              = r[19]
        partname            = r[20]
        pc                  = r[21]
        commercial          = r[22]
        sampleflg           = r[23]
        orderorgi           = r[24]
        orderround          = r[25]
        firmflg             = r[26]
        shippedflg          = r[27]
        shippedqty          = r[28]
        ordermonth          = r[29]
        balqty              = r[30]
        bidrfl              = r[31]
        deleteflg           = r[32]
        ordertype           = r[33]
        reasoncd            = r[34]
        upddte              = r[35]
        updtime             = r[36]
        carriercode         = r[37]
        bioabt              = r[38]
        bicomd              = r[39]
        bistdp              = r[40]
        binewt              = r[41]
        bigrwt              = r[42]
        bishpc              = r[43]
        biivpx              = r[44]
        bisafn              = r[45]
        biwidt              = r[46]
        bihigh              = r[47]
        bileng              = r[48]
        lotno               = r[49]
        minimum             = r[50]
        maximum             = r[51]
        picshelfbin         = r[52]
        stkshelfbin         = r[53]
        ovsshelfbin         = r[54]
        picshelfbasicqty    = r[55]
        outerpcs            = r[56]
        allocateqty         = r[57]

        cust_id = PsDb().get_fetch_one(f"select id from tbt_customers where bishpc='{bishpc}'")
        fac_id = PsDb().get_fetch_one(f"select id from tbt_tag_groups where title='{factory}'")
        ship_id = PsDb().get_fetch_one(f"select id from tbt_ship_types where title='{shiptype}'")
        zone_id = PsDb().get_fetch_one(f"select id from tbt_zone_ids where tag_id='{fac_id}' and bioat='{bioabt}'")

        sql_ship = f""
        if shiptype == "A":
            sql_ship = f"and ship_air=true"

        elif shiptype == "B":
            sql_ship = f"and ship_boat=true"

        elif shiptype == "T":
            sql_ship = f"and ship_truck=true"

        customer = PsDb().get_fetch_all(f"select id,group_order from tbt_territories where cust_id='{cust_id}' and tag_id='{fac_id}' {sql_ship}")

        order_substr = "ALL"
        if customer[0][1] == "E":
            order_substr = pono[len(pono) - 3:]

        elif customer[0][1] == "F":
            order_substr = pono[:3]
            if str(pono).find("#") == 0:
                order_substr = pono[:4]
            if str(pono).find("@") == 0:
                order_substr = pono[:4]
        else:
            order_substr = "ALL"

        sql_order_header = f"""select id from tbt_order_headers where tag_id='{fac_id}' and cust_id='{customer[0][0]}' and ship_id='{ship_id}' and zone_id='{zone_id}' and group_no='{order_substr}' and etd='{str(etdtap)[:10]}' and pc='{pc}' and commercials='{commercial}' and order_status='0'"""
        print(sql_order_header)
        order_header_uuid = PsDb().get_fetch_one(sql_order_header)

        sql_insert_header = f"""update tbt_order_headers set gedi_id='{gedi_id}',sync=false,updated_at=current_timestamp where id='{order_header_uuid}'"""
        if order_header_uuid is False:
            sql_insert_header = f"""insert into tbt_order_headers
            (id, gedi_id, tag_id, cust_id, ship_id, zone_id, group_no, etd, pc, commercials, bioabt, ordertype, bicomd, biivpx, order_status, sync, created_at, updated_at)
            values(uuid_generate_v4(), '{gedi_id}', '{fac_id}', '{customer[0][0]}', '{ship_id}', '{zone_id}', '{order_substr}', '{str(etdtap)[:10]}', '{pc}', '{commercial}', {bioabt}, '{ordertype}', '{bicomd}','{biivpx}', '0', false, current_timestamp, current_timestamp)"""
        
        PsDb().excute_data(sql_insert_header)
        order_header_uuid = PsDb().get_fetch_one(sql_order_header)

        i += 1

if __name__ == '__main__':
    main()
    