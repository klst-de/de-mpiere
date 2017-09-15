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
		
		def TABLENAME = "ad_org"
		def rows = n_live_tup[TABLENAME]  	// rows in original
		def done = doMerge(TABLENAME,[TABLENAME+'_id'])
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"

		// alle nachfolgenden Inserts mit useSuperUser !!!
		// dh. sie müssen wiederholt werden, wenn ad_user geladen ist 
// wg. m_warehouse_id dropship_warehouse_id in ad_orginfo => m_warehouse, darin wiederum c_location_id
//  es genügt die paar zu laden ==> alle mit
//		  Detail: Schlüssel »(c_location_id)=(50002)« existiert bereits. -- es gibt in mierp viele < 1000000
//		==> die von GardenWord 50000 - auf mandanten 0 setzten

		def sql =  "UPDATE c_bpartner_location SET c_location_id=109 WHERE ad_client_id=? AND c_location_id>200"
		sqlInstance.execute(sql,[GERDENWORD_CLIENT_ID]) 
		updates = sqlInstance.getUpdateCount()
		println "${CLASSNAME}:run updates = ${updates} : ${sql}"
		sqlInstance.execute(sql,[DEFAULT_CLIENT_ID])
		updates = sqlInstance.getUpdateCount()
		println "${CLASSNAME}:run updates = ${updates} : ${sql}"


		sql =  "UPDATE m_warehouse SET c_location_id=109 WHERE ad_client_id=? AND c_location_id>200"
		sqlInstance.execute(sql,[GERDENWORD_CLIENT_ID]) 
		def	updates = sqlInstance.getUpdateCount()
		println "${CLASSNAME}:run updates = ${updates} : ${sql}"
		
//   Detail: Auf Schlüssel (c_location_id)=(1000002) wird noch aus Tabelle »m_warehouse« verwiesen.
		sql =  "UPDATE m_warehouse SET c_location_id=109 WHERE ad_client_id=? AND c_location_id=1000002"
		sqlInstance.execute(sql,[DEFAULT_CLIENT_ID]) 
		updates = sqlInstance.getUpdateCount()
		
		//	Detail: Regel _RETURN für Sicht rv_bpartner hängt von Spalte »address1« ab ...
		TABLENAME = "c_location"
		sql = makeDelete(TABLENAME) + "and c_location_id>200"
		println "${CLASSNAME}:run ${sql} \n"
		sqlInstance.execute(sql,[GERDENWORD_CLIENT_ID]) 
		def	deletes = sqlInstance.getUpdateCount()
		println "${CLASSNAME}:run deletes = ${deletes}."
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"

		TABLENAME = "m_warehouse"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"

		TABLENAME = "ad_orginfo"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"

		TABLENAME = "ad_client"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"

		// einiges an abhängigkeiten für ad_clientinfo:
/*

  c_calendar_id numeric(10,0),                 1000000
  c_acctschema1_id numeric(10,0),              1000000
  c_uom_volume_id numeric(10,0),
  c_uom_weight_id numeric(10,0),
  c_uom_length_id numeric(10,0),
  c_uom_time_id numeric(10,0),
  ad_tree_menu_id numeric(10,0),                10   --- das ist der von GW
  ad_tree_org_id numeric(10,0),                1000023 
  ad_tree_bpartner_id numeric(10,0),           1000022
  ad_tree_project_id numeric(10,0),            1000024
  ad_tree_salesregion_id numeric(10,0),        1000025;
  ad_tree_product_id numeric(10,0),            1000021;
  m_productfreight_id numeric(10,0),           1000000; REFERENCES m_product (m_product_id) MATCH SIMPLE
  c_bpartnercashtrx_id numeric(10,0),          1000000  REFERENCES c_bpartner (c_bpartner_id) MATCH SIMPLE
  keeplogdays numeric(10,0),
  ad_tree_activity_id numeric(10,0),           1000027  REFERENCES ad_tree (ad_tree_id) MATCH SIMPLE
  ad_tree_campaign_id numeric(10,0),           1000026  REFERENCES ad_tree (ad_tree_id) MATCH SIMPLE
  logo_id numeric(10,0),
  logoreport_id numeric(10,0),
  logoweb_id numeric(10,0),
 	
 */
		TABLENAME = "ad_clientinfo"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME,["ad_tree_org_id","ad_tree_bpartner_id","ad_tree_project_id","ad_tree_salesregion_id","ad_tree_product_id",
			"m_productfreight_id","c_bpartnercashtrx_id",
			"ad_tree_activity_id","ad_tree_campaign_id"]) // Spalten auf null setzten
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"

		// in c_currency_id:102 cursymbol: €=?=E  -- TODO
		// für ad_role  braucht es   c_currency_id OK und ad_tree_menu_id = 1000019 (Admin, alle)
		
		// Abhängigkeiten für ad_user:
/*
  c_bpartner_id numeric(10,0),                     -- auf NULL setzten später update   
  c_bpartner_location_id numeric(10,0),            -- auf NULL setzten später update  
OK  c_greeting_id numeric(10,0),
  ad_orgtrx_id numeric(10,0),                         >>NULL Relation »mierp001.ad_orgtrx« existiert nicht
OK  c_job_id numeric(10,0),
OK  c_location_id numeric(10,0) DEFAULT NULL::numeric,
OK  c_campaign_id numeric(10,0) DEFAULT NULL::numeric,   -- wurde beim setup angelegt
          salesrep_id numeric(10,0), ---> ad_user
  bp_location_id numeric(10,0) DEFAULT NULL::numeric,      -- Spalte »bp_location_id« existiert nicht in mierp
  ad_emailconfig_id numeric(10,0) DEFAULT NULL::numeric,   >>NULL Relation »mierp001.ad_emailconfig« existiert nicht
		
 */
		
		TABLENAME = "c_jobcategory"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME,)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"

		TABLENAME = "c_job"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"

		TABLENAME = "c_greeting"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = doInsert(TABLENAME,[],SYSTEM_CLIENT_ID)  // wg. Schlüssel (c_greeting_id)=(501634) ist nicht in Tabelle »c_greeting« vorhanden.
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"

		// Detail: Schlüssel (c_greeting_id)=(501634) ist nicht in Tabelle »c_greeting« vorhanden.
		// TODO bei c_greeting die mit ad_client_id=0 importieren
		TABLENAME = "ad_user"
		rows = n_live_tup[TABLENAME] 
		done = doInsert(TABLENAME,["c_bpartner_id","c_bpartner_location_id"]) // Spalten auf null setzten
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
/* hiernach
update ad_user set isactive='Y' where ad_user_id in(1000000)
update ad_user set c_bpartner_id=1000002 where ad_user_id in(1000000)
update ad_user set c_bpartner_id=1000001 where ad_user_id in(1000001)
 */
//		TABLENAME = "ad_user_roles"
//		TABLENAME = "ad_role"
//		TABLENAME = "r_requesttype"
//		TABLENAME = "r_statuscategory"
//		rows = n_live_tup[TABLENAME]
//		errs = fullMatcher.tablematcher(TABLENAME,[TABLENAME+'_id'])
//		println "${CLASSNAME}:run ${errs}/${rows} errors for table ${TABLENAME}.\n"

//		errs=errs+ match('ad_role')
//		errs=errs+ match('ad_user_substitute')
//		errs=errs+ match('c_jobcategory')
//		errs=errs+ match('c_jobassignment')
//		errs=errs+ match('c_bank') // sollte von mf kommen, also nur + , kein -
//		// CollaborationMgt:
//		errs=errs+ match('cm_chat')
//		errs=errs+ match('cm_chatentry')
//		// grosse:
////		errs=errs+ match('ad_user')
////		errs=errs+ match('c_bpartner') // +3
		return null;
	}

  // wird in eclipse benötigt, damit ein "Run As Groovy Script" möglich ist (ohne Inhalt)
  // nach dem Instanzieren wird run() ausgeführt
  static main(args) {
  }

}
