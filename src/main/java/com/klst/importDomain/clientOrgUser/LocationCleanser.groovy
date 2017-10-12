package com.klst.importDomain.clientOrgUser

import com.klst.importDomain.CleanserScript
import groovy.lang.Binding

class LocationCleanser extends CleanserScript {

	def CLASSNAME = this.getClass().getName()
	def TABLENAME = "c_location"
	public LocationCleanser() {
		println "${CLASSNAME}:ctor"
	}

	public LocationCleanser(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}

	
	def setRowsInactive = { subquery ->
		def sql = """
update c_bpartner_location set isactive = 'N'
where c_bpartner_id in( ${subquery} )
"""
		println "${CLASSNAME}:setInactive update =${sql}"
		sqlInstance.execute(sql,[])
		return sqlInstance.getUpdateCount()
	}
	
	def inactive = []

	@Override
	public Object run() {
		println "${CLASSNAME}:run connected to ${sqlInstance.getConnection().metaData.URL}"
		
		def tbl_ad_user = "ad_user"
//		inactive = getInactiv(tbl_ad_user)
//		def delUsr = deleteInactive(inactive,tbl_ad_user)
		
		// c_bpartner_location inaktivieren, die von inaktiven ad_user referenziet werden (es sind wenige)
		def subquery = getInactiv(tbl_ad_user,["c_bpartner_location_id"],true)
		def updted = setRowsInactive(subquery) // c_bpartner_location auskodiert
		println "${CLASSNAME}:run updted=${updted}"

		// c_bpartner_location inaktivieren, die von inaktiven c_bpartner referenziet werden
		def tbl = "c_bpartner"
		def subqueryBP = getInactiv(tbl,[],true)
		def updtedBP = setRowsInactive(subquery) // c_bpartner_location auskodiert
		println "${CLASSNAME}:run updted=${updtedBP}"
		
//		inactive = []
//		inactive = getInactiv("c_bpartner_location")
//		def delBpL = deleteInactive(inactive,"c_bpartner_location")
		
//		inactive = []
//		inactive = getInactiv(tbl)
//		def delBp = deleteInactive(inactive,tbl)
		
		// jetzt das eigentliche @see https://github.com/metasfresh/metasfresh-mi67-scripts/issues/14
		// TODO
		// es wird derzeit alles aus c_location nach mf importiert, ca 206Tsd nur 34Tsd wird benötigt
		// alle 206Tsd sind aktiv!
		// ==> alle deaktiveren
		// ==> LocationCleanser lauf : dabei wird versucht zu löschen was möglich ist
		// ==> nur die wieder aktivieren, die noch übrig geblieben sind
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
