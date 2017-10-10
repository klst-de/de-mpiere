package com.klst.importDomain.orderInvoice

import com.klst.importDomain.ImportScript
import groovy.lang.Binding;


class GlDocOrder extends ImportScript {

	def CLASSNAME = this.getClass().getName()
	
	public GlDocOrder() {
		println "${CLASSNAME}:ctor"
	}

	public GlDocOrder(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}

/* Untersuchung c_doctype
	 1. ein UPDATE wie "gl_category" ist m vll problematisch
	 2. es ist mit ad_sequence verzahnt
  select * from ad_sequence where ad_sequence_id in(
	select docnosequence_id from c_doctype where ad_client_id=1000000
  )
	 3. welche werden unbeding benötigt?
	 
  -- wie viele verschiedene gibt es in c_order?
  select c_doctype_id,count(*) from mierp001.c_order
  group by 1
  1000033;  364
  1000032;  137
  1000030; 9465
  1000031; 9556
  1000027; 2143
  1000016;18798
		0; 2815
  select c_doctype_id,count(*) from mierp001.c_invoice
  group by 1
  1000005;19651
  1000002;20277
  1000004;  707
  1000006;  622
  1000016;   13
		0;  166
  select c_doctype_id,count(*) from mierp001.c_payment
  group by 1
  1000009;    1
  1000008;22633
  
  select c_doctype_id,count(*) from mierp001.m_inout
  group by 1
  1000014;15001
  1000011;21951
  1000015;   30
  1000013;  107
  
  select c_doctype_id,count(*) from mierp001.m_inventory
  group by 1
  1000023;22
  
  ----------- was behindert ein UPDATE
  select m.name,m.issotrx,m.docbasetype,m.docsubtypeso, o.* from c_doctype o , mierp001.c_doctype m
  WHERE o.ad_client_id=1000000 --AND o.c_doctype_id in( SELECT c_doctype_id FROM mierp001.c_doctype )
  AND o.c_doctype_id=m.c_doctype_id -- JOIN
  AND m.isactive='Y'
  AND (o.docbasetype<>m.docbasetype or o.docsubtypeso<>m.docsubtypeso)
  --and m.issotrx='Y'
  AND o.c_doctype_id in( -- kommt in c_order vor
  1000033,1000032,1000030,1000031,1000027,1000016, -- und in c_invoice:
  1000005,1000002,1000004,1000006,1000016,
  1000008,1000009,             -- wg c_payment
  1000014,1000011,1000015,1000013, -- wg m_inout
  1000023                      -- wg m_inventory
  )
	  
  ---> derzeit nur ein m.c_doctype_id:
  "Barverkauf";"Y";"SOO";"WR";1000033 ===> würde "Manufacturing Order" "MOP" mappen , korrekt wäre o.c_doctype_id:1000039
  ===> vorher o.c_doctype_id's tauschen
  
------------------ belegnummern
select 'A',ad_sequence_id,name,startno,currentnext,currentnextsys from ad_sequence where ad_sequence_id in(
  select docnosequence_id from c_doctype where ad_client_id=1000000 and c_doctype_id in(1000033,1000032,1000030,1000031,1000027,1000016)
)
union
select 'm',ad_sequence_id,name,startno,currentnext,currentnextsys from mierp001.ad_sequence where ad_sequence_id in(
  select docnosequence_id from mierp001.c_doctype where ad_client_id=1000000 and c_doctype_id in(1000033,1000032,1000030,1000031,1000027,1000016)
)
order by 1,2
-- mierp: Angebote:546267 Aufträge:546270
"A";1000281;"Purchase Order";800000;800000;80000
"A";1000292;"Non binding offer";20000;20000;2000
"A";1000295;"Standard Order";50000;50000;5000
"A";1000296;"Credit Order";60000;60000;6000
"A";1000297;"Warehouse Order";70000;70000;7000
"A";1000304;"POS Order";80000;80000;8000
"m";546256;"Purchase Order"  ;800000;27295;80000
"m";546267;"Non binding offer";20000;36174;2000
"m";546270;"Standard Order"   ;50000;73186;5000

 */
  
	def	doctypes = { tablename="c_doctype" , schema=DEFAULT_FROM_SCHEMA ->
		println "${CLASSNAME}:doctypes for = ${schema}.${tablename}"
		def cols = [:]
		def sql_in = """
1000033,1000032,1000030,1000031,1000027,1000016,  -- in c_order vor
1000005,1000002,1000004,1000006,1000016,          -- in c_invoice
1000008,1000009,                                  -- wg c_payment
1000014,1000011,1000015,1000013,                  -- wg m_inout
1000023                                           -- wg m_inventory
"""
		def sql = """
SELECT * 
FROM ${schema}.${tablename}
WHERE ad_client_id=? 
AND isactive='Y'
AND ${tablename}_id in( ${sql_in} )
"""
		def update_sql = """
UPDATE ${tablename} o
 SET description = ( SELECT 'imported TODO' FROM ${schema}.${tablename} m WHERE o.${tablename}_id=m.${tablename}_id )
WHERE o.ad_client_id=? AND o.${tablename}_id in( ${sql_in} )
"""
		def done = doSql(update_sql,[DEFAULT_CLIENT_ID])
		
		update_sql = """
UPDATE ${tablename} o
 SET created = ( SELECT created FROM ${schema}.${tablename} m WHERE o.${tablename}_id=m.${tablename}_id )
WHERE o.ad_client_id=? AND o.${tablename}_id in( ${sql_in} )
"""
		done = doSql(update_sql,[DEFAULT_CLIENT_ID])
					
		update_sql = """
UPDATE ${tablename} o
 SET updated = ( SELECT updated FROM ${schema}.${tablename} m WHERE o.${tablename}_id=m.${tablename}_id )
WHERE o.ad_client_id=? AND o.${tablename}_id in( ${sql_in} )
"""
		done = doSql(update_sql,[DEFAULT_CLIENT_ID])
			
		def check_sql = """
SELECT o.${tablename}_id,o.docbasetype,m.docbasetype,o.issotrx,m.issotrx,o.docsubtypeso,m.docsubtypeso FROM ${tablename} o , ${schema}.${tablename} m
 WHERE o.${tablename}_id=m.${tablename}_id                     -- JOIN
   AND o.ad_client_id=?  AND o.${tablename}_id in( ${sql_in} )
   AND (o.docbasetype<>m.docbasetype OR 
        -- o.issotrx<>m.issotrx OR -- Ausschalten wg. c_doctype_id:1000013 , auf mierp001:Y 
        o.hasproforma<>m.hasproforma OR 
        o.c_doctypeshipment_id<>m.c_doctypeshipment_id OR 
        o.c_doctypeinvoice_id<>m.c_doctypeinvoice_id OR 
        o.isdocnocontrolled<>m.isdocnocontrolled OR 
        o.gl_category_id<>m.gl_category_id OR 
        o.docsubtypeso<>m.docsubtypeso) -- CHECK
"""
/*
  select * from ad_sequence where ad_sequence_id in(
	select docnosequence_id from c_doctype where ad_client_id=1000000
  )
 */
		sqlInstance.eachRow(check_sql,[DEFAULT_CLIENT_ID]) { row ->
			println "EXCEPTION: ${row}"
			throw new RuntimeException("docbasetype, issotrx, docsubtypeso EXCEPTION")
		}

		update_sql = """
UPDATE ad_sequence o
 SET currentnext = ( SELECT currentnext FROM ${schema}.ad_sequence m WHERE m.ad_sequence_id=? )
WHERE o.ad_client_id=? AND o.ad_sequence_id in( SELECT docnosequence_id FROM ${tablename} WHERE ${tablename}_id=? )
"""
			
		sqlInstance.eachRow(sql,[DEFAULT_CLIENT_ID]) { row ->
			if(row.getProperties().first) {
				// ...
			}
			println "${row}"
			//println "${row.docnosequence_id} ${DEFAULT_CLIENT_ID} ${row.c_doctype_id} ${update_sql}"
			done = doSql(update_sql,[row.docnosequence_id,DEFAULT_CLIENT_ID,row.c_doctype_id])
		}
		return cols.size()
	}

	@Override
	public Object run() {
		println "${CLASSNAME}:run"
		rowsPerTable()
		
		def done = 0
		def TABLENAME = "gl_category" // nur UPDATE
		def rows = n_live_tup[TABLENAME] 
		def update_sql = """
UPDATE gl_category o
 SET created = ( SELECT created FROM mierp001.gl_category m WHERE o.gl_category_id=m.gl_category_id )
WHERE o.ad_client_id=? AND o.gl_category_id in( SELECT gl_category_id FROM mierp001.gl_category )
"""
		done = doSql(update_sql,[DEFAULT_CLIENT_ID])
		
		update_sql = """
UPDATE gl_category o
 SET updated = ( SELECT updated FROM mierp001.gl_category m WHERE o.gl_category_id=m.gl_category_id )
WHERE o.ad_client_id=? AND o.gl_category_id in( SELECT gl_category_id FROM mierp001.gl_category )
"""
		done = doSql(update_sql,[DEFAULT_CLIENT_ID])
		
		update_sql = """
UPDATE gl_category o
 SET description = ( SELECT 'imported' FROM mierp001.gl_category m WHERE o.gl_category_id=m.gl_category_id )
WHERE o.ad_client_id=? AND o.gl_category_id in( SELECT gl_category_id FROM mierp001.gl_category )
"""
		done = doSql(update_sql,[DEFAULT_CLIENT_ID])

		// c_doctype_id's tauschen
		TABLENAME = "c_doctype"   
		rows = n_live_tup[TABLENAME]
		update_sql = """
UPDATE ${TABLENAME} SET c_doctype_id=     -2 WHERE ad_client_id=1000000 AND c_doctype_id=1000033;
UPDATE ${TABLENAME} SET c_doctype_id=1000033 WHERE ad_client_id=1000000 AND c_doctype_id=1000039;
UPDATE ${TABLENAME} SET c_doctype_id=1000039 WHERE ad_client_id=1000000 AND c_doctype_id=     -2;
"""
		done = doSql(update_sql)

		done = doctypes()
		// TODO ad_printformat

		TABLENAME = "c_activity"   
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "c_campaign"   
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "c_charge"   
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "c_conversiontype"  // leer 
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "c_project"   
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "c_order"  // description character varying(255), >>> muss auf 1024   
		// FEHLER: NULL-Wert in Spalte »m_pricelist_id« verletzt Not-Null-Constraint , Korrektur hier
		update_sql = """
UPDATE ${DEFAULT_FROM_SCHEMA}.${TABLENAME} SET m_pricelist_id=1000001 WHERE c_order_id=1021712;
"""
		done = doSql(update_sql)
		rows = n_live_tup[TABLENAME]
		// zwei cols vorerst auf null
		done = doInsert(TABLENAME,["c_cashline_id","c_payment_id"])
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		// Test: Frachtkostenberechnung/freightcostrule wird nicht angezeigt : ist aber mandatory und in DB vorhanden?!
		
		return null;
	}

  // wird in eclipse benötigt, damit ein "Run As Groovy Script" möglich ist (ohne Inhalt)
  // nach dem Instanzieren wird run() ausgeführt
  static main(args) {
  }

}
