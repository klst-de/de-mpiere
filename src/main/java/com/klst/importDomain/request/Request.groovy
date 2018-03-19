// wg. https://projects.klst.com/issues/1482
package com.klst.importDomain.request

import com.klst.importDomain.ImportScript
import groovy.lang.Binding;


class Request extends ImportScript {

	def CLASSNAME = this.getClass().getName()
	
	public Request() {
		println "${CLASSNAME}:ctor"
	}

	public Request(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}

	@Override
	public Object run() {
		println "${CLASSNAME}:run"
		rowsPerTable()
		
		def done = 0
		def TABLENAME = "r_statuscategory"
		def rows = n_live_tup[TABLENAME] 
				
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "r_status"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "r_requesttype"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)

		TABLENAME = "r_standardresponse"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)

		TABLENAME = "r_resolution"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
/*
                                                         r_statuscategory rows=5.
                                                         r_status rows=27.
                                                         r_requesttype rows=12.
                                                         r_standardresponse rows=5.
                                                         r_resolution rows=1.
                                                         r_request rows=3908.
                                                         r_requestaction rows=18860.
                                                         r_requestupdate rows=11956.

OK hier:  r_requesttype_id numeric(10,0) NOT NULL,       r_requesttype rows=12.
in Product.groovy:  r_group_id numeric(10,0),
in Product.groovy:  r_category_id numeric(10,0),
OK hier:  r_status_id numeric(10,0),
  r_resolution_id numeric(10,0),
  r_requestrelated_id numeric(10,0),
--		
in GlDocOrder.groovy: c_campaign_id numeric(10,0),
in GlDocOrder.groovy: c_order_id numeric(10,0),
in GlDocOrder.groovy: c_invoice_id numeric(10,0),
in GlDocOrder.groovy: c_payment_id numeric(10,0),
in Product.groovy:  m_product_id numeric(10,0),
in GlDocOrder.groovy: c_project_id numeric(10,0),
0  a_asset_id numeric(10,0),
in GlDocOrder.groovy: m_inout_id numeric(10,0),
0  m_rma_id numeric(10,0),
--  r_mailtext_id numeric(10,0),  50000 wird nicht importiert
OK hier:  r_standardresponse_id numeric(10,0),                  r_standardresponse rows=5.
in Product.groovy:  m_productspent_id numeric(10,0), -- REFERENCES mierp001.m_product
in GlDocOrder.groovy:  c_activity_id numeric(10,0),
in GlDocOrder.groovy:  c_invoicerequest_id numeric(10,0), REFERENCES mierp001.c_invoice
0  m_changerequest_id numeric(10,0),
0  m_fixchangenotice_id numeric(10,0), REFERENCES mierp001.m_changenotice
  outlookknopf character(1) DEFAULT NULL::bpchar, TODO ? kann outlookknopf auch virtuell sein ===> NEIN 
  
 */
		TABLENAME = "r_request"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "r_requestaction"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "r_requestupdate"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "r_requestupdates"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		
		TABLENAME = "ad_archive"
		def delete_sql = """
DELETE FROM ${DEFAULT_FROM_SCHEMA}.${TABLENAME} 
 WHERE ad_client_id=1000000
 AND ad_archive_id IN( 1000212,1000404,1001793 )
"""
		done = doSql(delete_sql)
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "ad_attachment"
		delete_sql = """
DELETE FROM ${DEFAULT_FROM_SCHEMA}.${TABLENAME} 
 WHERE ad_client_id=1000000
 AND ad_attachment_id IN( 1000001,1000011,1000136,1002501 )
"""
		done = doSql(delete_sql)
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "ad_attachmentnote" // eigentlich keine Daten hier
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)

		// wg. https://projects.klst.com/issues/1020#note-3
		TABLENAME = "ad_searchdefinition"
		def insert_sql = """
INSERT INTO ${DEFAULT_TO_SCHEMA}.${TABLENAME} ( 
  ad_client_id,
  ad_column_id,
  ad_org_id,
  ad_searchdefinition_id,
  ad_table_id,
  ad_window_id,
  created,
  createdby,
  datatype,
  description,
  isactive,
  name,
  query,
  searchtype,
  transactioncode,
  updated,
  updatedby,
  po_window_id,
  isdefault
)
    select 
  ad_client_id,
  ad_column_id,
  ad_org_id,
  ad_searchdefinition_id,
  ad_table_id,
  ad_window_id,
  created,
  createdby,
  datatype,
  description,
  isactive,
  name,
  query,
  searchtype,
  transactioncode,
  updated,
  updatedby,
  po_window_id,
  isdefault
      from ${DEFAULT_FROM_SCHEMA}.${TABLENAME}
     WHERE ad_searchdefinition_id>=1000000		
"""
		done = doSql(insert_sql)
		done = updateSequence(TABLENAME)
		
		return null;	
	}

  // wird in eclipse benötigt, damit ein "Run As Groovy Script" möglich ist (ohne Inhalt)
  // nach dem Instanzieren wird run() ausgeführt
  static main(args) {
  }

}
