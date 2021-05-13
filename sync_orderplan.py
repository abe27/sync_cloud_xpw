from yazaki_packages.db import PsDb, OraDB, WmsDb
from yazaki_packages.cloud import SplCloud

def main():
    sql = f"""select id,gedi_id,seq,vendor,cd,unit,whs,tagrp,factory,sortg1,sortg2,sortg3,plantype,orderid,pono,recid,biac,shiptype,etdtap,partno,partname,pc,commercial,sampleflg,orderorgi,orderround,firmflg,shippedflg,shippedqty,ordermonth,balqty,bidrfl,deleteflg,ordertype,reasoncd,upddte,updtime,carriercode,bioabt,bicomd,bistdp,binewt,bigrwt,bishpc,biivpx,bisafn,biwidt,bihigh,bileng,lotno,minimum,maximum,picshelfbin,stkshelfbin,ovsshelfbin,picshelfbasicqty,outerpcs,allocateqty,sync,created_at,updated_at 
        from tbt_order_datas 
        where sync=false
        order by created_at,gedi_id,seq"""

    gedi_id = WmsDb().get_fetch_one(f"select file_id from tbm_filegedis where file_name='NONE'")
    docs = PsDb().get_fetch_all(sql)
    i = 0
    while i < len(docs):
        r = docs[i]
        ord_id              = r[0]
        # gedi_id             = r[1]
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
        partname            = str(r[20]).replace("'", "'''")
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


        # create order header
        user_id             =   0
        terr_id             =   None
        fac_id              =   None
        cust_id             =   None
        fac_title           =   None
        group_title         =   None

        cust_obj = WmsDb().get_fetch_all(f"""select terr_id,id,fac_id,cust_id,fac_title,b.order_group_title from tbm_territories t
            inner join tbm_customers c on t.terr_customer_id = c.cust_id
            inner join tbm_factorys f on t.terr_cust_factory_id = f.fac_id
            inner join auth_user u on t.terr_user_id = u.id
            inner join tbm_ordergroupbys b on t.terr_cust_ordergroup_id = b.order_group_id 
            where c.consignee='{bishpc}' and t.terr_case_shipment like '%{shiptype}%'""")

        if cust_obj is None:
            cust_obj = WmsDb().get_fetch_all(f"""select terr_id,id,fac_id,cust_id,fac_title,b.order_group_title from tbm_territories t
                inner join tbm_customers c on t.terr_customer_id = c.cust_id
                inner join tbm_factorys f on t.terr_cust_factory_id = f.fac_id
                inner join auth_user u on t.terr_user_id = u.id
                inner join tbm_ordergroupbys b on t.terr_cust_ordergroup_id = b.order_group_id 
                where c.consignee='{bishpc}' and t.terr_case_shipment like '%-%'""")

        for o in cust_obj:
            terr_id             =   o[0]
            user_id             =   o[1]
            fac_id              =   o[2]
            cust_id             =   o[3]
            fac_title           =   o[4]
            group_title         =   o[5]

            WmsDb().excute_data(f"""update tbm_territories set terr_cust_prefix='{biivpx}',terr_updated_at=current_timestamp where terr_id='{terr_id}'""")
        
        if user_id != 0:
            shiptype_id             =   WmsDb().get_fetch_one(f"select ship_id from tbm_shiptypes where ship_title='{shiptype}'")
            zone_id                 =   WmsDb().get_fetch_one(f"""select zone_id from tbm_zones where zone_factoryid_id='{fac_id}' and zone_shipment_id='{shiptype_id}'  and zone_value='{bioabt}'""")

            order_id                =   pono
            if group_title == "E":
                order_id = pono[len(pono) - 3:]

            elif group_title == "F":
                order_id = pono[:3]
                if pono[:1] == "@" or pono[:1] == "#":
                    order_id = pono[:4]

            else:
                order_id     =   "ALL"
                

            reasoncd_id = WmsDb().get_fetch_one(f"select rv_id from tbm_revisetypes where rv_title='-'")
            txt = str(f"{reasoncd}").strip()
            if txt != "":
                txt = txt[:1]
                reasoncd_id = WmsDb().get_fetch_one(f"select rv_id from tbm_revisetypes where rv_title='{txt}'")
                
            whs_id = WmsDb().get_fetch_one(f"select whs_id from tbm_whs where whs_title='C'")
            # order_id_key = __PsFetchOne(f"select uuid_generate_v4()")
            sql_insert_order = f"""insert into tbt_orderplans(ord_plan_id, ord_plan_grpordno, ord_plan_etdtap, ord_plan_shippedflg, ord_plan_pc, ord_plan_commercial, ord_plan_sampflg, ord_plan_ordertype, ord_plan_bidrfl, ord_plan_bioabt, ord_plan_firmflg, ord_plan_biivpx, ord_plan_poupdflag, ord_plan_item, ord_plan_ctn, ord_plan_invoice, ord_plan_grp_reason, ord_plan_sync, ord_plan_split, ord_plan_created_at, ord_plan_updated_at, ora_plan_shiptype_id, ora_plan_whs_id, ora_plan_zone_id, ord_plan_custid_id, ord_plan_file_gedi_id, user_id)
            values(uuid_generate_v4(), '{order_id}', '{etdtap}', '{shippedflg}', '{pc}', '{commercial}', '{sampleflg}', '{ordertype}', '{bidrfl}',  {bioabt}, '{firmflg}', '{biivpx}', '', 0, 0, false, false, false, false, current_timestamp, current_timestamp, '{shiptype_id}', '{whs_id}', '{zone_id}', '{terr_id}', '{gedi_id}', '{user_id}')"""
            sql_order = f"""
                select ord_plan_id from tbt_orderplans where 
                ord_plan_grpordno='{order_id}' and 
                ord_plan_etdtap='{etdtap}' and 
                ord_plan_commercial='{commercial}' and 
                ora_plan_shiptype_id='{shiptype_id}' and 
                ora_plan_whs_id='{whs_id}' and 
                ora_plan_zone_id='{zone_id}' and 
                ord_plan_custid_id='{terr_id}' and 
                user_id='{user_id}'"""

            order_uuid_id = False
            if txt == "M" or txt == "D" or txt == "S" or txt == "L" or txt == "Q" or txt == "1" or txt == "2":
                order_uuid_id = WmsDb().get_fetch_one(sql_order)
            
            
            if order_uuid_id != False:
                sql_insert_order = f"""update tbt_orderplans
                set ord_plan_grpordno='{order_id}', ord_plan_etdtap='{etdtap}', ord_plan_shippedflg='{shippedflg}', ord_plan_pc='{pc}', ord_plan_commercial='{commercial}', ord_plan_sampflg='{sampleflg}', ord_plan_ordertype='{ordertype}', ord_plan_bidrfl='{bidrfl}', ord_plan_bioabt={bioabt}, ord_plan_firmflg='{firmflg}', ord_plan_biivpx='{biivpx}', ord_plan_invoice=false, ord_plan_grp_reason=false,ord_plan_sync=false,ord_plan_updated_at=current_timestamp, ora_plan_shiptype_id='{shiptype_id}', ora_plan_whs_id='{whs_id}', ora_plan_zone_id='{zone_id}', ord_plan_custid_id='{terr_id}', ord_plan_file_gedi_id='{gedi_id}', user_id={user_id}, ord_plan_status=false, ord_plan_split=false
                where ord_plan_id='{order_uuid_id}'"""

            WmsDb().excute_data(sql_insert_order)

            order_uuid_id = WmsDb().get_fetch_one(sql_order)

            partid_id = WmsDb().get_fetch_one(f"select part_id from tbm_parts where part_no='{partno}'")
            if partid_id is False:
                part_type_id = WmsDb().get_fetch_one(f"select part_type_id from tbm_parttypes where part_type_title='COIL'")
                if fac_title == "INJ":
                    part_type_id = WmsDb().get_fetch_one(f"select part_type_id from tbm_parttypes where part_type_title='PART'")

                WmsDb().excute_data(f"""insert tbm_parts(part_id, part_no, part_desc, part_kind, part_size, part_color, part_dim_width, part_dim_length, part_dim_height, part_dim_weight, part_status, part_created_at, part_updated_at, part_type_id)
                values(uuid_generate_v4(), '{partno}', '{partname}', '', '', '', 0, 0, 0, 0, false, current_timestamp, current_timestamp, '{part_type_id}')""")
                partid_id = WmsDb().get_fetch_one(f"select part_id from tbm_parts where part_no='{partno}'")

            else:
                WmsDb().excute_data(f"""update tbm_parts set part_desc='{str(partname).strip()}',part_updated_at=current_timestamp where part_id='{partid_id}'""")

            insert_order_body = f"""insert into tbt_orderplanbodys(ord_body_id, ord_body_orderno, ord_body_ordermonth, ord_body_orderorgi, ord_body_orderround, ord_body_balqty, ord_body_shippedflg, ord_body_shippedqty, ord_body_sampflg, ord_body_carriercode, ord_body_allocateqty, ord_body_bidrfl, ord_body_deleteflg, ord_body_reasoncd, ord_body_bicomd, ord_body_bistdp, ord_body_bileng, ord_body_biwidt, ord_body_bihigh, ord_body_binewt, ord_body_bigrwt, ord_body_lotno, ord_body_status, ord_body_recreate_status, ord_body_split, ord_body_adddata, ord_body_inv_created, ord_body_sync, ord_body_created_at, ord_body_updated_at, ord_body_grpordno_id, ord_body_partid_id, ord_body_revise_id)
            values(uuid_generate_v4(), '{str(pono).strip()}', '{ordermonth}', {orderorgi}, {orderround}, {balqty}, '{shippedflg}', {shippedqty}, '{sampleflg}', '{carriercode}', {allocateqty}, '{bidrfl}', '{deleteflg}', '{reasoncd}', '{bicomd}', {bistdp}, {bileng}, {biwidt}, {bihigh}, {binewt}, {bigrwt}, '{lotno}', 0, 0, false, false, false, false, current_timestamp, current_timestamp, '{order_uuid_id}', '{partid_id}', '{reasoncd_id}')"""
            if txt != "":
                order_body_id = WmsDb().get_fetch_one(f"select ord_body_id from tbt_orderplanbodys where ord_body_grpordno_id='{order_uuid_id}' and ord_body_partid_id='{partid_id}'")
                if order_body_id != False:
                    insert_order_body = f"""update tbt_orderplanbodys
                    set ord_body_balqty={balqty}, ord_body_reasoncd='{reasoncd}', ord_body_lotno='{lotno}', ord_body_sync=false, ord_body_updated_at=current_timestamp, ord_body_revise_id='{reasoncd_id}'
                    where ord_body_id='{order_body_id}'"""

                WmsDb().excute_data(insert_order_body)

            else:
                WmsDb().excute_data(insert_order_body)

            print(f"{order_uuid_id} order partid: {partid_id}")


        # cust_id = PsDb().get_fetch_one(f"select id from tbt_customers where bishpc='{bishpc}'")
        # fac_id = PsDb().get_fetch_one(f"select id from tbt_tag_groups where title='{factory}'")
        # ship_id = PsDb().get_fetch_one(f"select id from tbt_ship_types where title='{shiptype}'")
        # zone_id = PsDb().get_fetch_one(f"select id from tbt_zone_ids where tag_id='{fac_id}' and bioat='{bioabt}'")

        # sql_ship = f""
        # if shiptype == "A":
        #     sql_ship = f"and ship_air=true"

        # elif shiptype == "B":
        #     sql_ship = f"and ship_boat=true"

        # elif shiptype == "T":
        #     sql_ship = f"and ship_truck=true"

        # customer = PsDb().get_fetch_all(f"select id,group_order from tbt_territories where cust_id='{cust_id}' and tag_id='{fac_id}' {sql_ship}")

        # order_substr = "ALL"
        # if customer[0][1] == "E":
        #     order_substr = pono[len(pono) - 3:]

        # elif customer[0][1] == "F":
        #     order_substr = pono[:3]
        #     if str(pono).find("#") == 0:
        #         order_substr = pono[:4]
        #     if str(pono).find("@") == 0:
        #         order_substr = pono[:4]
        # else:
        #     order_substr = "ALL"

        # sql_order_header = f"""select id from tbt_order_headers where tag_id='{fac_id}' and cust_id='{customer[0][0]}' and ship_id='{ship_id}' and zone_id='{zone_id}' and group_no='{order_substr}' and etd='{str(etdtap)[:10]}' and pc='{pc}' and commercials='{commercial}' and order_status='0'"""
        # print(sql_order_header)
        # order_header_uuid = PsDb().get_fetch_one(sql_order_header)

        # sql_insert_header = f"""update tbt_order_headers set gedi_id='{gedi_id}',sync=false,updated_at=current_timestamp where id='{order_header_uuid}'"""
        # if order_header_uuid is False:
        #     sql_insert_header = f"""insert into tbt_order_headers
        #     (id, gedi_id, tag_id, cust_id, ship_id, zone_id, group_no, etd, pc, commercials, bioabt, ordertype, bicomd, biivpx, order_status, sync, created_at, updated_at)
        #     values(uuid_generate_v4(), '{gedi_id}', '{fac_id}', '{customer[0][0]}', '{ship_id}', '{zone_id}', '{order_substr}', '{str(etdtap)[:10]}', '{pc}', '{commercial}', {bioabt}, '{ordertype}', '{bicomd}','{biivpx}', '0', false, current_timestamp, current_timestamp)"""
        
        # PsDb().excute_data(sql_insert_header)
        # order_header_uuid = PsDb().get_fetch_one(sql_order_header)

        # update status sync
        PsDb().excute_data(f"update tbt_order_datas set sync=true where id='{ord_id}'")
        i += 1

if __name__ == '__main__':
    main()
    