package com.klst.importDomain.product

import com.klst.importDomain.CleanserScript
import groovy.lang.Binding

class InventoryCleanser extends CleanserScript {

	def CLASSNAME = this.getClass().getName()
	def TABLENAME = "m_inventory"
	
	public InventoryCleanser() {
		println "${CLASSNAME}:ctor"
	}

	public InventoryCleanser(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}

	// m_warehouse_id=1000000 -- "Paulinenstraße"
	def subSelectWhere = """
  WHERE (updated < TO_TIMESTAMP('2017-04-05 00:00:00','YYYY-MM-DD HH24:MI:SS'))
   or m_warehouse_id=1000000 
"""

	def subSelect = """
  select ${TABLENAME}_id FROM ${TABLENAME} ${subSelectWhere}
"""

	def inventorylineFrom = """
from m_inventoryline
where m_inventoryline_id in( ${subSelect} )
"""
	
	def inventorylineSelect = """
SELECT m_inventoryline_id ${inventorylineFrom}
"""
	
	@Override
	public Object run() {
		println "${CLASSNAME}:run connected to ${sqlInstance.getConnection().metaData.URL}"
		
		println "${CLASSNAME}:run query zur Info: ${inventorylineSelect}"
		
		def done = 0
		done = doSql("DELETE from m_inventorylinema where m_inventoryline_id in( ${inventorylineSelect} )")
		println "${CLASSNAME}:run ${done} deleted."
		
		done = doSql("DELETE from m_costdetail where m_inventoryline_id in( ${inventorylineSelect} )")
		println "${CLASSNAME}:run ${done} deleted."

		done = doSql("DELETE from m_transaction where m_inventoryline_id in( ${inventorylineSelect} )")
		println "${CLASSNAME}:run ${done} deleted."

		done = doSql("DELETE ${inventorylineFrom}")
		println "${CLASSNAME}:run ${done} deleted."
		
		done = doSql("UPDATE ${TABLENAME} SET isactive='N' ${subSelectWhere}")
		inactiveIDs = getInactiv(TABLENAME)
		println "${CLASSNAME}:run inactiveIDs #${inactiveIDs.size}" 
		done = tryToDelete(inactiveIDs,TABLENAME)
		done = doSql("UPDATE ${TABLENAME} SET isactive='Y' ${subSelectWhere}")
/* ???? seltsam
com.klst.importDomain.product.InventoryCleanser:doSql org.postgresql.util.PSQLException: FEHLER: Aktualisieren oder Löschen in Tabelle »m_inventoryline« verletzt Fremdschlüssel-Constraint »minventoryline_mcostdetail« von Tabelle »m_costdetail«
  Detail: Auf Schlüssel (m_inventoryline_id)=(1008481) wird noch aus Tabelle »m_costdetail« verwiesen.
com.klst.importDomain.product.InventoryCleanser:doSql Transaction rollback.	  
 */
		return this;
	}


  // wird in eclipse benötigt, damit ein "Run As Groovy Script" möglich ist (ohne Inhalt)
  // nach dem Instanzieren wird run() ausgeführt
  static main(args) {
  }
  
}
