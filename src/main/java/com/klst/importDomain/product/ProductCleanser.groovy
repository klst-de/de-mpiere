package com.klst.importDomain.product

import com.klst.importDomain.CleanserScript
import groovy.lang.Binding

/* Notizen für AD
 
1. m_product_po wird nicht in mf importiert -> https://github.com/metasfresh/metasfresh-mi67-scripts/issues/59#issuecomment-315859345

bezug hyper-v mierp001: m_product_po

 * es gibt zu viele, denn es gibt auch die für unaktive Produkte

select count(*) from m_product_po -- #47544
-- delete from m_product_po
where m_product_id in( select m_product_id from m_product where isactive='N' ) -- inaktiv #18291 , active #29253
-- Query returned successfully: 18291 rows affected, 176 msec execution time.

 * nur die m_product_po für unaktive Produkte importieren
 * lt. NW werden gar keine in mf benötigt!? ==> https://github.com/metasfresh/metasfresh-mi67-scripts/issues/66
 
bezug hyper-v mierp001: Produktpreise

 * es gibt zu viele, denn es gibt auch die für unaktive Produkte

select count(*) from M_ProductPrice -- #96754
where m_product_id in( select m_product_id from m_product where isactive='Y' ) -- inaktiv #36594 , active #60160

 * diese lassen sich nicht löschen, denn unter M_ProductPrice hängt m_productscaleprice
 * select usescaleprice,count(*) from M_ProductPrice group by 1 => kann alles gelöscht werden.
 
 * also m_productscaleprice löschen (alle) oder hiermit nur die für m_product where isactive='N' :
delete from m_productscaleprice where m_productprice_id in(
--select count(*) from m_productscaleprice where m_productprice_id in( -- inaktiv #2648 , active #12022
  select m_productprice_id from M_ProductPrice
  where m_product_id in( select m_product_id from m_product where isactive='N' )
)
 * jetzt
delete from M_ProductPrice
where m_product_id in( select m_product_id from m_product where isactive='N' )
-- Query returned successfully: 36594 rows affected, 02:14 minutes execution time.

 */
class ProductCleanser extends CleanserScript {

	def CLASSNAME = this.getClass().getName()
	def TABLENAME = "m_product"
	
	public ProductCleanser() {
		println "${CLASSNAME}:ctor"
	}

	public ProductCleanser(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}

	// inactive prod-Einträge in table fact_acct vor 2013 : #6145 rows
/* wie viele sind es? nach Jahr:

	select EXTRACT(isoyear FROM dateacct),count(*) 
	  from fact_acct fa 
	where M_Product_ID in( select M_Product_ID from M_Product where isactive='N' )
	group by 1
	order by 1
-- liefert:
	2011;3221		<== diese löschen, da prod inactiv
	2012;2924		<== diese löschen, da prod inactiv
	2013; 198		<== diese löschen, da prod inactiv
	2014;  84
	2015;  38
	2016;  18
	2017;  61
*/	
	def getFact_acctProd = { tablename , keycolumn=[] , retquery=false ->
		if(keycolumn==[]) {
			keycolumn = "${tablename}_id"
		} else if(keycolumn instanceof java.util.List && keycolumn.size()==1) {
			keycolumn = keycolumn[0]
		} else {
			println "${CLASSNAME}:getInactiv PRIMARY KEY = ${keycolumn}."
		}
		
		def sql = """
select ${keycolumn}  
from fact_acct fa where ${keycolumn} in( select ${keycolumn} from ${tablename} where isactive='N' )
and EXTRACT(isoyear FROM dateacct) < 2014 
"""
		if(retquery) {
			//println "${CLASSNAME}:getInactiv query = ${sql}."
			return sql
		}
		def inactive = []
		sqlInstance.eachRow(sql,[]) { row ->
			inactive.add(row[0])
		}
		println "${CLASSNAME}:getInactiv found ${inactive.size()} inactive rows in ${tablename}."
		return inactive
	}

	
	@Override
	public Object run() {
		println "${CLASSNAME}:run connected to ${sqlInstance.getConnection().metaData.URL}"
		
		def query = getInactiv(TABLENAME,[],true) // tablename , keycolumn=[] , retquery=false
		println "${CLASSNAME}:run query=${query}" // das ergibt eine query -- nur zum test
		def inactiveIDs = []
		
		inactiveIDs = getInactiv(TABLENAME)
//		println "${CLASSNAME}:run inactiveIDs #${inactiveIDs.size}" // #17710
//		def dels = deleteInactive(inactiveIDs,TABLENAME) // das bereinigt nicht viele (-> getFact_acctProd und diese löschen ... ) deleted/tries=574/18284 in table m_product.

		/* daher folgendes Vorgehen:
		   in diversen (abhängigen) Tabellen die recs mit inactiveIDs löschen, wenn möglich
		   - m_costdetail
		   - m_cost
		   - m_transaction
		   - m_inventoryline
		   - zum Schluss fact_acct aber mit inactiveIDs = getFact_acctProd(TABLENAME)
		   jetzt sind mehr inaktive prods in m_products zum Löschen bereit
		 */
		
		def dels = 0
		dels = tryToDelete(inactiveIDs,"m_costdetail"   ,["m_product_id"],true) // deleted/tries=3061/6145 in table m_costdetail.
		dels = tryToDelete(inactiveIDs,"m_cost"         ,["m_product_id"],true) // deleted/tries=1988/6145 in table m_cost.
		dels = tryToDelete(inactiveIDs,"m_transaction"  ,["m_product_id"],true) // deleted/tries=2355/6145 in table m_transaction
		dels = tryToDelete(inactiveIDs,"m_inventoryline",["m_product_id"],true) // deleted/tries= 714/6145 in table m_inventoryline.
		
		inactiveIDs = getFact_acctProd(TABLENAME) // ca 6000
		println "${CLASSNAME}:run inactiveIDs #${inactiveIDs.size}" 
		dels = tryToDelete(inactiveIDs,"fact_acct"      ,["m_product_id"],true) // deleted/tries=6371/6145 in table fact_acct.

		// jetzt sollten mehr inaktive prods gelöscht werden
		inactiveIDs = getInactiv(TABLENAME)
		println "${CLASSNAME}:run inactiveIDs #${inactiveIDs.size}" // #17710
		dels = deleteInactive(inactiveIDs,TABLENAME)

		return this;
	}


  // wird in eclipse benötigt, damit ein "Run As Groovy Script" möglich ist (ohne Inhalt)
  // nach dem Instanzieren wird run() ausgeführt
  static main(args) {
  }
  
}
