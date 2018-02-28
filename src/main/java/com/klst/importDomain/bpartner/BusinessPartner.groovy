package com.klst.importDomain.bpartner

import com.klst.importDomain.ImportScript
import groovy.lang.Binding;
import java.sql.SQLException


class BusinessPartner extends ImportScript {

	def CLASSNAME = this.getClass().getName()
	
	public BusinessPartner() {
		println "${CLASSNAME}:ctor"
	}

	public BusinessPartner(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}

	/* ohne die 3:
1000000;1000000;1000000;"N";"2011-02-04 16:28:14";100;"2017-11-24 16:37:02";1000016;"Standard";"Schnoor, Eike Christian";"";"";"N";1000000;"N";"Y";"N";"Y";"N";"N";"";"";"";"";"";"N";;"";0;;"";"";0;0;0;0;"";0;0;;;;;"N";"";"";"";;;;;;"";"";"";"";;"N";;;"X";;;;0;"";;0;;;"Y";"Y";"";;1000000;;;;"N";"";"";;"N";;"N";"";"";"";"Y";;"";"";"";"Su";"";"";"";""
1000001;1000000;1000000;"N";"2011-02-04 16:28:17";100;"2017-11-24 16:38:41";1000016;"clientUser";"clientUser";"";"";"N";1000000;"N";"Y";"N";"Y";"Y";"Y";"";"";"";"";"";"N";;"";0;;"";"";0;0;0;0;"";0;0;;;;;"N";"";"";"";;;;;;"";"";"";"";;"N";;;"X";;;;0;"";;0;;;"Y";"Y";"";;1000000;;;;"N";"";"";;"N";;"N";"";"";"";"Y";;"";"";"";"Su";"";"";"";""
1000002;1000000;1000000;"N";"2011-02-04 16:28:18";100;"2017-11-24 16:38:48";1000016;"clientAdmin";"clientAdmin";"";"";"N";1000000;"N";"Y";"N";"Y";"Y";"Y";"";"";"";"";"";"N";;"";0;;"";"";0;0;0;0;"";0;0;;;;;"N";"";"";"";;;;;;"";"";"";"";;"N";;;"X";;;;0;"";;0;;;"Y";"Y";"";;1000000;;;;"N";"";"";;"N";;"N";"";"";"";"Y";;"";"";"";"Su";"";"";"";""
	 */
	def	makeInsertBpartner = { tablename , nullcolumns=[] , useSuperUser=false , SuperUserId=SUPER_USER_ID->
		def selo = (commonKeys.collect  { "$it" } as Iterable).join(', ')
		def selm = (commonKeys.collect  { "m.$it" } as Iterable).join(', ')
		if(useSuperUser) { // use SuperUserId for cols
			selm = selm.replaceFirst("m.createdby", SuperUserId.toString()).replaceFirst("m.updatedby", SuperUserId.toString())
		}
		println "${CLASSNAME}:makeInsert size ${nullcolumns.size()}"
		nullcolumns.each { selm = selm.replaceFirst("m.${it}", "null") }
		println "${CLASSNAME}:makeInsert ${selm}"
		def sql = """
INSERT INTO ${tablename} ( ${selo} ) 
    select ${selm} from ${DEFAULT_FROM_SCHEMA}.${tablename} AS m
     WHERE m.ad_client_id=?
       AND m.c_bpartner_id NOT IN(1000000,1000001,1000002)
"""
		return sql
	}
	

	def doInsertBpartner = { tablename , nullcolumns=[] , clientID=DEFAULT_CLIENT_ID , useSuperUser=false ->
		println "${CLASSNAME}:doInsertBpartner ${tablename} nullcolumns = ${nullcolumns}."
		if(n_live_tup.get(tablename)==null) {
			println "${CLASSNAME}:doInsertBpartner ${tablename} leer -> nix zu tun."
			return 0
		}
		origin = columns(tablename,DEFAULT_FROM_SCHEMA)
		target = columns(tablename)
		if(origin.size()==target.size()) {
			println "${CLASSNAME}:doInsertBpartner PASSED number of columns = ${origin.size()}."
		} else {
			println "${CLASSNAME}:doInsertBpartner differnt number of columns: origin = ${origin.size()} <> target = ${target.size()}."
			println "${origin.size()}: ${origin}"
			println "${target.size()}: ${target}"
			println "${CLASSNAME}:doInsertBpartner trying the intersection ..."
		}
		if(checkColumns()!=null) {
			println "${CLASSNAME}:doInsertBpartner PASSED number of intersect columns = ${commonKeys.size()}."
		} else {
			throw new RuntimeException("keine matching Spalten gefunden")
		}
		
		//return makeInsert(tablename,nullcolumns) //test
		// NICHT löschen
		def sql = "NICHT löschen" //makeDelete(tablename)
		println "${sql}"

		def deletes = 0
		def inserts = 0
		sqlInstance.connection.autoCommit = false
		try {
			//sqlInstance.execute(sql,[clientID])
			//deletes = sqlInstance.getUpdateCount()
			println "${CLASSNAME}:doInsertBpartner KEINE deletes = ${deletes}."
				
			sql = makeInsertBpartner(tablename,nullcolumns,useSuperUser)
			println "${sql}"
			sqlInstance.execute(sql,[clientID])
			inserts = sqlInstance.getUpdateCount()
			println "${CLASSNAME}:doInsertBpartner inserts = ${inserts}."
			
			sqlInstance.commit();
		}catch(SQLException ex) {
			println "${CLASSNAME}:doInsertBpartner ${ex}"
			sqlInstance.rollback()
			println "${CLASSNAME}:doInsertBpartner Transaction rollback."
		}

		return inserts-deletes
	}

	def	c_bankaccount_acct_insert = { c_bankaccount_id ->
		def sql = """
INSERT INTO c_bankaccount_acct ( 
  c_bankaccount_id,
  c_acctschema_id,
  ad_client_id,
  ad_org_id,
  createdby,
  updatedby,
  b_intransit_acct,
  b_asset_acct,
  b_expense_acct,
  b_interestrev_acct,
  b_interestexp_acct,
  b_unidentified_acct,
  b_unallocatedcash_acct,
  b_paymentselect_acct,
  b_settlementgain_acct,
  b_settlementloss_acct,
  b_revaluationgain_acct,
  b_revaluationloss_acct
)
    select ?,c_acctschema_id,ad_client_id,ad_org_id,?,?,b_intransit_acct,b_asset_acct, b_expense_acct,b_interestrev_acct,b_interestexp_acct,
           b_unidentified_acct,b_unallocatedcash_acct,b_paymentselect_acct,b_settlementgain_acct,b_settlementloss_acct,b_revaluationgain_acct,b_revaluationloss_acct
      from c_bankaccount_acct
     WHERE ad_client_id=1000000 AND c_bankaccount_id=1000000		
"""
		return doSql(sql , [c_bankaccount_id,SUPER_USER_ID,SUPER_USER_ID])
	}

	def	c_tax_acct_insert = { c_tax_id ->
		def sql = """
INSERT INTO c_tax_acct ( 
  c_tax_id,
  c_acctschema_id,
  ad_client_id,
  ad_org_id,
  createdby,
  updatedby,
  t_due_acct,
  t_liability_acct,
  t_credit_acct,
  t_receivables_acct,
  t_expense_acct
)
    select ?,c_acctschema_id,ad_client_id,ad_org_id,?,?,t_due_acct,t_liability_acct,t_credit_acct,t_receivables_acct,t_expense_acct
      from c_tax_acct
     WHERE ad_client_id=1000000 AND c_tax_id=1000000		
"""
		return doSql(sql , [c_tax_id,SUPER_USER_ID,SUPER_USER_ID])
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
		//done = doInsert(TABLENAME) // raus, damit c_bp_customer_acct c_bp_employee_acct c_bp_vendor_acct nicht zerstört werden
		//   doMerge geht nicht because: FEHLER: als Ausdruck verwendete Unteranfrage ergab mehr als eine Zeile
		done = doInsertBpartner(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		// Update 1000000:"Schnoor, Eike Christian"
		done = doUpdate(TABLENAME,[TABLENAME+'_id'],1000000)
		
		// "c_bp_group" nur std : raus, damit c_bp_group_acct nicht gelöscht wird
		
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

		drop_cbank_cbankaccount = """
ALTER TABLE c_bankaccount DROP CONSTRAINT cbank_cbankaccount
"""
		add_cbank_cbankaccount = """
ALTER TABLE c_bankaccount
  ADD CONSTRAINT cbank_cbankaccount FOREIGN KEY (c_bank_id)
      REFERENCES c_bank (c_bank_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
"""
		done = doSql(drop_cbank_cbankaccount)
		
		TABLENAME = "c_bank"  // die bankdaten sind in mi nicht aktuell
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME) 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = doInsert(TABLENAME,[],SYSTEM_CLIENT_ID,true)  // viele bankdaten sind unter SYSTEM_CLIENT 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "c_bankaccount"  
		rows = n_live_tup[TABLENAME]
		//done = doInsert(TABLENAME) // raus, damit c_bankaccount_acct nicht gelöscht wird
		done = doMerge(TABLENAME,[TABLENAME+'_id'],['c_bankaccount_id'])
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		done = doSql(add_cbank_cbankaccount)
		// c_bankaccount_acct row für das Konto 1000001 erstellen
		TABLENAME = "c_bankaccount_acct"
		done = c_bankaccount_acct_insert(1000001) // "3055111"
		done = updateSequence_acct("c_bankaccount") // tablename ohne acct!

		TABLENAME = "c_bp_bankaccount"  
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME) 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		drop_ctaxcategory_ctax = """
ALTER TABLE c_tax DROP CONSTRAINT ctaxcategory_ctax
"""
		add_ctaxcategory_ctax = """
ALTER TABLE c_tax
  ADD CONSTRAINT ctaxcategory_ctax FOREIGN KEY (c_taxcategory_id)
      REFERENCES c_taxcategory (c_taxcategory_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
"""
		done = doSql(drop_ctaxcategory_ctax)
		
		TABLENAME = "c_taxcategory" 
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "c_tax" 
		rows = n_live_tup[TABLENAME]
		//done = doInsert(TABLENAME) // raus, damit c_tax_acct nicht gelöscht wird
		done = doMerge(TABLENAME,[TABLENAME+'_id'])
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		done = doSql(add_ctaxcategory_ctax)
		
		// c_tax_acct rows für tax>1000001 erstellen
		TABLENAME = "c_tax_acct"
		done = c_tax_acct_insert(1000001) // "19% Deutschland MwSt."
		done = c_tax_acct_insert(1000002) 
		done = c_tax_acct_insert(1000003) 
		done = c_tax_acct_insert(1000004) 
		done = c_tax_acct_insert(1000005) 
		done = c_tax_acct_insert(1000006) 
		done = c_tax_acct_insert(1000007) 
		done = c_tax_acct_insert(1000008) 
		done = c_tax_acct_insert(1000009) 
		done = updateSequence_acct("c_tax") // tablename ohne acct!
		
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
