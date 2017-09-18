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
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		// TODO currentnextsys errechnen
		done = doSql("update ad_sequence set currentnext = (select max(AD_User_id)+1 from AD_User) where name = 'AD_User'")

		return null;
	}

  // wird in eclipse benötigt, damit ein "Run As Groovy Script" möglich ist (ohne Inhalt)
  // nach dem Instanzieren wird run() ausgeführt
  static main(args) {
  }

}
