package com.klst.importDomain.bpartner

import com.klst.importDomain.ImportScript
import groovy.lang.Binding;


class BusinessPartner extends ImportScript {

	def CLASSNAME = this.getClass().getName()
	
	public BusinessPartner() {
		println "${CLASSNAME}:ctor"
	}

	public BusinessPartner(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}

	@Override
	public Object run() {
		println "${CLASSNAME}:run"
		rowsPerTable()
		
		def TABLENAME = "c_paymentterm"
		def rows = n_live_tup[TABLENAME] 
		def done = 0
		done = doInsert(TABLENAME) 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)

		TABLENAME = "m_pricelist"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME) 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "m_discountschema"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME) 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "c_bpartner"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME) 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "c_bp_group"  // nur std
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME) 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "c_salesregion"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME) 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "c_bpartner_location"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME) 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)

		def ad_user_update_sql = """
UPDATE ad_user o
 SET c_bpartner_id = ( SELECT c_bpartner_id FROM mierp001.ad_user m WHERE o.ad_user_id=m.ad_user_id )
WHERE o.ad_client_id=1000000
"""
		done = doSql(ad_user_update_sql)
		
		ad_user_update_sql = """
UPDATE ad_user o
 SET c_bpartner_location_id = ( SELECT c_bpartner_location_id FROM mierp001.ad_user m WHERE o.ad_user_id=m.ad_user_id )
WHERE o.ad_client_id=1000000
"""
		done = doSql(ad_user_update_sql)
		
		ad_user_update_sql = """
UPDATE ad_user o
SET createdby = ( SELECT m.createdby FROM mierp001.ad_user m WHERE o.ad_user_id=m.ad_user_id )
WHERE o.ad_client_id=1000000
"""
		done = doSql(ad_user_update_sql)
		
		ad_user_update_sql = """
UPDATE ad_user o
SET updatedby = ( SELECT m.updatedby FROM mierp001.ad_user m WHERE o.ad_user_id=m.ad_user_id )
WHERE o.ad_client_id=1000000
"""
		done = doSql(ad_user_update_sql)
		
		TABLENAME = "c_bp_relation" // c_bp_relation.name character varying(60) NOT NULL >>> muss auf 255
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME) 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
  
		TABLENAME = "c_bank"  // die bankdaten sind in mi nicht aktuell, kein swiftcode - die von mf sind besser (ABER ob sie mit mi matchen?)
		                      // Überprüft: die matchenden recs haben keinen swiftcode bei mf, ==>  mf daten sind gar nicht besser
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME) 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = doInsert(TABLENAME,[],SYSTEM_CLIENT_ID,true)  // viele bankdaten sind unter SYSTEM_CLIENT 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "c_bankaccount"  
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME) 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		// Schlüssel (c_bank_id)=(9916489) ist nicht in Tabelle »c_bank« vorhanden.
		TABLENAME = "c_bp_bankaccount"  
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME) 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
// TODO cash erst nach c_invoice		
//		TABLENAME = "c_cash" 
//		rows = n_live_tup[TABLENAME]
//		done = doInsert(TABLENAME)
//		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
//		done = updateSequence(TABLENAME)
//
//		TABLENAME = "c_cashline" 
//		rows = n_live_tup[TABLENAME]
//		done = doInsert(TABLENAME)
//		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
//		done = updateSequence(TABLENAME)

		TABLENAME = "c_taxcategory" 
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "c_tax" 
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "r_interestarea" 
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "r_contactinterest" // nach r_interestarea
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"

		return null;
	}

  // wird in eclipse benötigt, damit ein "Run As Groovy Script" möglich ist (ohne Inhalt)
  // nach dem Instanzieren wird run() ausgeführt
  static main(args) {
  }

}
