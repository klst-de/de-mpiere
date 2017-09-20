package com.klst.importDomain.clientOrgUser

import com.klst.importDomain.ImportScript
import groovy.lang.Binding;


class ClientOrgUser extends ImportScript {

	def CLASSNAME = this.getClass().getName()
	public ClientOrgUser() {
		println "${CLASSNAME}:ctor"
	}

	public ClientOrgUser(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}

	@Override
	public Object run() {
		println "${CLASSNAME}:run"
		rowsPerTable()
		
		// org , client (später) mit update, jetzt mit doMerge , die *info dazu nicht importieren
		def TABLENAME = "ad_org"
		def rows = n_live_tup[TABLENAME]
		//def done = doInsert(TABLENAME,[],DEFAULT_CLIENT_ID,true) // true == useSuperUser
		def done = doMerge(TABLENAME,[TABLENAME+'_id'])
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"

		TABLENAME = "ad_client"
		rows = n_live_tup[TABLENAME]
		//done = doInsert(TABLENAME,[],DEFAULT_CLIENT_ID,true)
		done = doMerge(TABLENAME,[TABLENAME+'_id'])
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		
		// die location 109 ist "40 Old Tannery Rd"
		done = doSql("UPDATE c_bpartner_location SET c_location_id=109 WHERE c_location_id>200")
		done = doSql("UPDATE m_warehouse         SET c_location_id=109 WHERE c_location_id>200")
		
		//	Detail: Regel _RETURN für Sicht rv_bpartner hängt von Spalte »address1« ab ...
		TABLENAME = "c_location"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME,[],DEFAULT_CLIENT_ID,true)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		// TODO currentnextsys errechnen
		done = doSql("update ad_sequence set currentnext = (select max(C_Location_id)+1 from C_Location) where name = 'C_Location'")
		
		TABLENAME = "m_warehouse"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME,[],DEFAULT_CLIENT_ID,true)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = doSql("update ad_sequence set currentnext = (select max(M_Warehouse_id)+1 from M_Warehouse) where name = 'M_Warehouse'")
		
		TABLENAME = "c_jobcategory"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME,[],DEFAULT_CLIENT_ID,true)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"

		TABLENAME = "c_job"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME,[],DEFAULT_CLIENT_ID,true)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = doSql("update ad_sequence set currentnext = (select max(C_Job_id)+1 from C_Job) where name = 'C_Job'")
		
		TABLENAME = "c_greeting"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME,[],SYSTEM_CLIENT_ID,true) // wg. Schlüssel (c_greeting_id)=(501634) 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = doInsert(TABLENAME,[],DEFAULT_CLIENT_ID,true)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		// TODO currentnextsys errechnen
		done = doSql("update ad_sequence set currentnext = (select max(C_Greeting_id)+1 from C_Greeting) where name = 'C_Greeting'")

		TABLENAME = "ad_user"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME,["c_bpartner_id","c_bpartner_location_id"],DEFAULT_CLIENT_ID,true)  // 2 Spalten auf null setzten
/* später nachholen:
UPDATE ad_user o
SET c_bpartner_id = ( SELECT c_bpartner_id FROM mierp001.ad_user m WHERE o.ad_user_id=m.ad_user_id )
WHERE o.ad_client_id=1000000
--
UPDATE ad_user o
SET c_bpartner_location_id = ( SELECT c_bpartner_location_id FROM mierp001.ad_user m WHERE o.ad_user_id=m.ad_user_id )
WHERE o.ad_client_id=1000000
 */
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		// TODO currentnextsys errechnen
		done = doSql("update ad_sequence set currentnext = (select max(AD_User_id)+1 from AD_User) where name = 'AD_User'")
// ----------------- statt doInsert: wg. der leeren menues
/*
--DELETE FROM ad_role 
--     WHERE ad_client_id=?
--
INSERT INTO ad_role ( ad_role_id, ad_client_id, ad_org_id, isactive, created, createdby, updated, name, updatedby, 
                      description, userlevel, c_currency_id, amtapproval, ad_tree_menu_id, 
                      ismanual, isshowacct, ispersonallock, ispersonalaccess, iscanexport, iscanreport, supervisor_id, iscanapproveowndoc, isaccessallorgs, ischangelog, preferencetype, overwritepricelimit, isuseuserorgaccess, ad_tree_org_id, 
                      confirmqueryrecords, maxqueryrecords, connectionprofile, allow_info_account, allow_info_asset, allow_info_bpartner, allow_info_cashjournal, allow_info_inout, allow_info_invoice, allow_info_order, allow_info_payment, allow_info_product, allow_info_resource, allow_info_schedule, userdiscount, allow_info_mrp, allow_info_crp, isdiscountuptolimitprice, isdiscountallowedontotal ) 
     select m.ad_role_id, m.ad_client_id, m.ad_org_id, m.isactive, m.created, 100, m.updated, m.name, 100, 
                      m.description, m.userlevel, m.c_currency_id, m.amtapproval, null,
                      m.ismanual, m.isshowacct, m.ispersonallock, m.ispersonalaccess, m.iscanexport, m.iscanreport, m.supervisor_id, m.iscanapproveowndoc, m.isaccessallorgs, m.ischangelog, m.preferencetype, m.overwritepricelimit, m.isuseuserorgaccess, m.ad_tree_org_id, 
                      m.confirmqueryrecords, m.maxqueryrecords, m.connectionprofile, m.allow_info_account, m.allow_info_asset, m.allow_info_bpartner, m.allow_info_cashjournal, m.allow_info_inout, m.allow_info_invoice, m.allow_info_order, m.allow_info_payment, m.allow_info_product, m.allow_info_resource, m.allow_info_schedule, m.userdiscount, m.allow_info_mrp, m.allow_info_crp, m.isdiscountuptolimitprice, m.isdiscountallowedontotal 
       from mierp001.ad_role AS m
--     WHERE m.ad_client_id=?
WHERE m.ad_role_id in( 1000004 , 1000003 , 1000002 )


 */
//		done = doSql("update ad_sequence set currentnext = (select max(AD_Role_id)+1 from AD_Role) where name = 'AD_Role'")
//		
//		TABLENAME = "ad_user_roles"
//		rows = n_live_tup[TABLENAME]
//		done = doInsert(TABLENAME,[],DEFAULT_CLIENT_ID,true) 
//		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
//		
//		TABLENAME = "ad_role_orgaccess"
//		rows = n_live_tup[TABLENAME]
//		done = doInsert(TABLENAME,[],DEFAULT_CLIENT_ID,true)
//		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"

		// isloginuser -- muss sein wg. login TODO
		done = doSql("UPDATE ad_user SET isloginuser='Y' , value=name WHERE ad_client_id=? AND password is not null",[DEFAULT_CLIENT_ID])
		
		return null;
	}

  // wird in eclipse benötigt, damit ein "Run As Groovy Script" möglich ist (ohne Inhalt)
  // nach dem Instanzieren wird run() ausgeführt
  static main(args) {
  }

}
