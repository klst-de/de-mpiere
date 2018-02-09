package com.klst.importDomain.bpartner

import com.klst.importDomain.CleanserScript
import groovy.lang.Binding

class BankCleanser extends CleanserScript {

	def CLASSNAME = this.getClass().getName()
	def TABLENAME = "c_bank"
	public BankCleanser() {
		println "${CLASSNAME}:ctor"
	}

	public BankCleanser(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}

	def setRowsInactive = { subquery ->
		def sql = """
update c_bank set isactive='N'
"""
		println "${CLASSNAME}:setInactive update =${sql}"
		sqlInstance.execute(sql,[])
		return sqlInstance.getUpdateCount()
	}
	
	def setRowsActive = { subquery ->
		def sql = """
update c_bank set isactive='Y'
where c_bank_id in( ${subquery} )
"""
		println "${CLASSNAME}:setInactive update =${sql}"
		sqlInstance.execute(sql,[])
		return sqlInstance.getUpdateCount()
	}

	def inactive = []

	@Override
	public Object run() {
		println "${CLASSNAME}:run connected to ${sqlInstance.getConnection().metaData.URL}"
		
		// alle 20Tsd sind aktiv!
		// welche Banken sind Kandidaten zum löschen? Der c_bp_bankaccount FOREIGN KEY CONSTRAINT verhindert das löschen nicht.
		// ==> Kandidate deaktiveren : setRowsInactive + setRowsActive
		// ==> deleteInactive : dabei wird versucht zu löschen was möglich ist
		// ==> TODO die wieder aktivieren, die noch übrig geblieben sind, wenn überhaupt
		
		def updted = setRowsInactive("all") 
		println "${CLASSNAME}:run updted=${updted}"

		updted = setRowsActive("select distinct c_bank_id from c_bp_bankaccount") 
		println "${CLASSNAME}:run updted=${updted}"

		inactive = []
		inactive = getInactiv(TABLENAME)
		def delBp = deleteInactive(inactive,TABLENAME)

		return null;
	}

  // wird in eclipse benötigt, damit ein "Run As Groovy Script" möglich ist (ohne Inhalt)
  // nach dem Instanzieren wird run() ausgeführt
  static main(args) {
  }

}
