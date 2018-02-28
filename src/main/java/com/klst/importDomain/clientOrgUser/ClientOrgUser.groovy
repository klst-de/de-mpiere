package com.klst.importDomain.clientOrgUser

import com.klst.importDomain.ImportScript
import groovy.lang.Binding;
import java.sql.SQLException


class ClientOrgUser extends ImportScript {

	def CLASSNAME = this.getClass().getName()
	public ClientOrgUser() {
		println "${CLASSNAME}:ctor"
	}

	public ClientOrgUser(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}

	//  
	//def doInsertWarehouse = { tablename , nullcolumns=[] , clientID=DEFAULT_CLIENT_ID , useSuperUser=false ->
	def doMergeWarehouse = { tablename , keycolumns=[] , nameValue=NAMEVALUE ->
		println "${CLASSNAME}:doMergeWarehouse ${tablename} PRIMARY KEY = ${keycolumns}."
		if(n_live_tup.get(tablename)==null) {
			println "${CLASSNAME}:doMergeWarehouse ${tablename} leer -> nix zu tun."
			return 0
		}
		origin = columns(tablename,DEFAULT_FROM_SCHEMA)
		target = columns(tablename)
		if(origin.size()==target.size()) {
			println "${CLASSNAME}:doMergeWarehouse PASSED number of columns = ${origin.size()}."
		} else {
			println "${CLASSNAME}:doMergeWarehouse differnt number of columns: origin = ${origin.size()} <> target = ${target.size()}."
			println "${origin.size()}: ${origin}"
			println "${target.size()}: ${target}"
			println "${CLASSNAME}:doMergeWarehouse trying the intersection ..."
		}
		if(checkColumns(false,nameValue)!=null) {
			println "${CLASSNAME}:doMergeWarehouse PASSED number of intersect columns = ${commonKeys.size()}."
		} else {
			throw new RuntimeException("keine matching Spalten gefunden")
		}
		def sql = makeMerge(tablename , keycolumns)
		println "${sql}"
		
		def updates = 0
		sqlInstance.connection.autoCommit = false
		try {
			sqlInstance.execute(sql,[DEFAULT_CLIENT_ID,DEFAULT_CLIENT_ID])
			updates = sqlInstance.getUpdateCount()
			println "${CLASSNAME}:doMerge updates = ${updates}."
			sqlInstance.commit();
		}catch(SQLException ex) {
			println "${CLASSNAME}:doMerge ${ex}"
			sqlInstance.rollback()
			println "${CLASSNAME}:doMerge Transaction rollback."
		}
		return updates
	}

	def	m_warehouse_acct_insert_sql = { m_warehouse_id ->
		def sql = """
INSERT INTO m_warehouse_acct ( 
  m_warehouse_id,
  c_acctschema_id,
  ad_client_id,
  ad_org_id,
  createdby,
  updatedby,
  w_inventory_acct,
  w_invactualadjust_acct,
  w_differences_acct,
  w_revaluation_acct 
)
    select ?,c_acctschema_id,ad_client_id,ad_org_id,?,?,w_inventory_acct,w_invactualadjust_acct, w_differences_acct,w_revaluation_acct
      from m_warehouse_acct
     WHERE ad_client_id=1000000 AND m_warehouse_id=1000000		
"""
		return doSql(sql , [m_warehouse_id,SUPER_USER_ID,SUPER_USER_ID])
	}

	@Override
	public Object run() {
		println "${CLASSNAME}:run"
		rowsPerTable()
		
		def rows = 0
		def done = 0
		// org , client (später) mit update, jetzt mit doMerge , die *info dazu nicht importieren
		def TABLENAME = "ad_org"
		rows = n_live_tup[TABLENAME]
		//def done = doInsert(TABLENAME,[],DEFAULT_CLIENT_ID,true) // true == useSuperUser
		done = doMerge(TABLENAME,[TABLENAME+'_id'])
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
		done = updateSequence(TABLENAME)
		
		TABLENAME = "c_jobcategory"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME,[],DEFAULT_CLIENT_ID,true)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "c_job"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME,[],DEFAULT_CLIENT_ID,true)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "c_greeting"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME,[],SYSTEM_CLIENT_ID,true) // wg. Schlüssel (c_greeting_id)=(501634) 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = doInsert(TABLENAME,[],DEFAULT_CLIENT_ID,true)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)

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
		done = updateSequence(TABLENAME)
		
		TABLENAME = "ad_role"
		def ad_role_insert_sql = """
INSERT INTO ad_role ( ad_role_id, ad_client_id, ad_org_id, isactive, created, createdby, updated, name, updatedby, 
                      description, userlevel, c_currency_id, amtapproval, ad_tree_menu_id, 
                      ismanual, isshowacct, ispersonallock, ispersonalaccess, iscanexport, iscanreport, supervisor_id, iscanapproveowndoc, isaccessallorgs, ischangelog, preferencetype, overwritepricelimit, isuseuserorgaccess, ad_tree_org_id, 
                      confirmqueryrecords, maxqueryrecords, connectionprofile, allow_info_account, allow_info_asset, allow_info_bpartner, allow_info_cashjournal, allow_info_inout, allow_info_invoice, allow_info_order, allow_info_payment, allow_info_product, allow_info_resource, allow_info_schedule, userdiscount, allow_info_mrp, allow_info_crp, isdiscountuptolimitprice, isdiscountallowedontotal ) 
     select m.ad_role_id, m.ad_client_id, m.ad_org_id, m.isactive, m.created, 100, m.updated, m.name, 100, 
                      m.description, m.userlevel, m.c_currency_id, m.amtapproval, null,
                      m.ismanual, m.isshowacct, m.ispersonallock, m.ispersonalaccess, m.iscanexport, m.iscanreport, m.supervisor_id, m.iscanapproveowndoc, m.isaccessallorgs, m.ischangelog, m.preferencetype, m.overwritepricelimit, m.isuseuserorgaccess, m.ad_tree_org_id, 
                      m.confirmqueryrecords, m.maxqueryrecords, m.connectionprofile, m.allow_info_account, m.allow_info_asset, m.allow_info_bpartner, m.allow_info_cashjournal, m.allow_info_inout, m.allow_info_invoice, m.allow_info_order, m.allow_info_payment, m.allow_info_product, m.allow_info_resource, m.allow_info_schedule, m.userdiscount, m.allow_info_mrp, m.allow_info_crp, m.isdiscountuptolimitprice, m.isdiscountallowedontotal 
       from mierp001.ad_role AS m
WHERE m.ad_role_id in( 1000004 , 1000003 , 1000002 )
"""
		done = doSql(ad_role_insert_sql)
		done = updateSequence(TABLENAME)
		
		TABLENAME = "ad_user_roles"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME,[],DEFAULT_CLIENT_ID,true) 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		
		TABLENAME = "ad_role_orgaccess"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME,[],DEFAULT_CLIENT_ID,true)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"

		// isloginuser + value -- muss sein wg. login
		done = doSql("UPDATE ad_user SET isloginuser='Y' , value=name WHERE ad_client_id=? AND password is not null",[DEFAULT_CLIENT_ID])
		
		// update  createdby  updatedby
		def c_location_update_sql = """
UPDATE c_location o
SET createdby = ( SELECT m.createdby FROM mierp001.c_location m WHERE o.c_location_id=m.c_location_id )
WHERE o.ad_client_id=1000000
"""
		done = doSql(c_location_update_sql)
		
		c_location_update_sql = """
UPDATE c_location o
SET updatedby = ( SELECT m.updatedby FROM mierp001.c_location m WHERE o.c_location_id=m.c_location_id )
WHERE o.ad_client_id=1000000		
"""
		done = doSql(c_location_update_sql)

		TABLENAME = "m_warehouse"
		// Bug: M_Warehouse_Acct wird mit doInsert gelöscht
		//      M_Warehouse_Acct rows für die anderen 3 Lager erstellen
/* TABLE m_warehouse_acct ...
  w_inventory_acct numeric(10,0) NOT NULL,
  w_invactualadjust_acct numeric(10,0) NOT NULL,
  w_differences_acct numeric(10,0) NOT NULL,
  w_revaluation_acct numeric(10,0) NOT NULL,
...
  CONSTRAINT m_warehouse_warehouse_acct FOREIGN KEY (m_warehouse_id)
      REFERENCES m_warehouse (m_warehouse_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
 */
		rows = n_live_tup[TABLENAME]
//		done = doInsert(TABLENAME,[],DEFAULT_CLIENT_ID,true)
		done = doMergeWarehouse(TABLENAME,[TABLENAME+'_id'])
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "m_warehouse_acct"
		done = m_warehouse_acct_insert_sql(1000001) // "Streckengeschäft"
		done = m_warehouse_acct_insert_sql(1000002) // "Lager Zeppelinpark"
		done = m_warehouse_acct_insert_sql(1000003) // "Lager Ostpreussendamm"
		done = updateSequence_acct("m_warehouse") // tablename ohne acct!
	
		return null;
	}

  // wird in eclipse benötigt, damit ein "Run As Groovy Script" möglich ist (ohne Inhalt)
  // nach dem Instanzieren wird run() ausgeführt
  static main(args) {
  }

}
