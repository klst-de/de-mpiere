package com.klst.importDomain.product

import com.klst.importDomain.ImportScript
import groovy.lang.Binding;


class Product extends ImportScript {

	def CLASSNAME = this.getClass().getName()
	
	public Product() {
		println "${CLASSNAME}:ctor"
	}

	public Product(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}

	@Override
	public Object run() {
		println "${CLASSNAME}:run"
		rowsPerTable()
		
		// "m_pricelist" und "m_discountschema" bereits importiert
/* bei uom herrscht ein chaos, diese müssen extra vorbereitet werden da SYSTEM_CLIENT_ID
 
--select * from mierp001.c_uom  
select c_uom_id,count(*) from mierp001.m_product where c_uom_id in
(100
,540002
,540000
,107
,104
,108
,1000000
,103
,1000001
,106
,101
,102
,105
,540003
,540001
,1000004
)
group by 1

    100;26542
    101;21
    102;12
           -- TODO einfügen
 540002;2       
 540003;3636
1000000;1
1000004;1 

 */
		def sql = """
 		INSERT INTO c_uom ( c_uom_id, ad_client_id, ad_org_id, isactive, created, updated, createdby, updatedby, x12de355, uomsymbol, name, description, stdprecision, costingprecision, isdefault, uomtype ) 
    select m.c_uom_id, m.ad_client_id, m.ad_org_id, m.isactive, m.created, m.updated, m.createdby, m.updatedby, m.x12de355, m.uomsymbol, m.name, m.description, m.stdprecision, m.costingprecision, m.isdefault, m.uomtype 
      from mierp001.c_uom AS m
     WHERE m.ad_client_id=?
       AND c_uom_id in(540002,540003,1000000,1000004)
"""
		done = doSql(sql,[SYSTEM_CLIENT_ID])
		def TABLENAME = "c_uom"
		def rows = n_live_tup[TABLENAME] 
		def done = 0
		done = doInsert(TABLENAME) 
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "c_uom_conversion"  // abhängig von m_product , aber leer 
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "m_product_category"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "m_attributeset"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "m_locator"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		//   Detail: Schlüssel »(m_product_id)=(148)« existiert bereits.
/*
select * from mierp001.m_product 
where m_product_id in(select m_product_id from  m_product)

139;"10000139";"XLR Kabel, 3m, St/Ku,"
140;"10000140";"XLR Kabel, 1m, St/Ku,"
141;"10000141";"Video/Audio Extender Cat.5/6 bis 600m,"
142;"10000142";"Professional HDMI Professional Extender"
145;"10000145";"Professional HDMI Professional Extender"
130;"10000130";"Batterie, Lith. für Apple Computer"
147;"10000147";"Professional HDMI Professional Extender"
148;"10000148";"Professional HDMI Professional Extender"
128;"10000128";"An- oder Abfahrt wegen Hardware"

die prods sind auf mi inaktiv
*/
		// provisorisch:
		sql = """
INSERT INTO m_product ( m_product_id, ad_client_id, ad_org_id, isactive, created, createdby, updated, updatedby, value, name, description, documentnote, help, upc, sku, c_uom_id, salesrep_id, issummary, isstocked, ispurchased, issold, isbom, isinvoiceprintdetails, ispicklistprintdetails, isverified, c_revenuerecognition_id, m_product_category_id, classification, volume, weight, shelfwidth, shelfheight, shelfdepth, unitsperpallet, c_taxcategory_id, s_resource_id, discontinued, discontinuedby, processing, s_expensetype_id, producttype, imageurl, descriptionurl, guaranteedays, r_mailtext_id, versionno, m_attributeset_id, m_attributesetinstance_id, downloadurl, m_freightcategory_id, m_locator_id, guaranteedaysmin, iswebstorefeatured, isselfservice, c_subscriptiontype_id, isdropship, isexcludeautodelivery, group1, group2, istoformule, lowlevel, unitsperpack ) 
    select m.m_product_id, m.ad_client_id, m.ad_org_id, m.isactive, m.created, m.createdby, m.updated, m.updatedby, m.value, m.name, m.description, m.documentnote, m.help, m.upc, m.sku, m.c_uom_id, m.salesrep_id, m.issummary, m.isstocked, m.ispurchased, m.issold, m.isbom, m.isinvoiceprintdetails, m.ispicklistprintdetails, m.isverified, m.c_revenuerecognition_id, m.m_product_category_id, m.classification, m.volume, m.weight, m.shelfwidth, m.shelfheight, m.shelfdepth, m.unitsperpallet, m.c_taxcategory_id, m.s_resource_id, m.discontinued, m.discontinuedby, m.processing, m.s_expensetype_id, m.producttype, m.imageurl, m.descriptionurl, m.guaranteedays, m.r_mailtext_id, m.versionno, m.m_attributeset_id, m.m_attributesetinstance_id, m.downloadurl, m.m_freightcategory_id, m.m_locator_id, m.guaranteedaysmin, m.iswebstorefeatured, m.isselfservice, m.c_subscriptiontype_id, m.isdropship, m.isexcludeautodelivery, m.group1, m.group2, m.istoformule, m.lowlevel, m.unitsperpack from mierp001.m_product AS m
     WHERE m.ad_client_id=1000000 and m_product_id not in
(139
,140
,141
,142
,145
,130
,147
,148
,128
,1000000 )
"""
		done = doSql(sql)
		
//               auf AD390 c_taxcategory_id numeric(10,0) NOT NULL, das ist OK , aber Steuerkategorie wird in Fenste Produkt nicht angezeigt!!!
//               Ursache: fehlende Übersetzungen  C_TaxCategory_Trl_de_DE.xml 

//-- wg. m_product_po
//		sql = """
//DELETE FROM m_product_po 
// WHERE ad_client_id=1000000
//"""
//		done = doSql(sql)
		
		sql = """
INSERT INTO m_product_po ( m_product_id, c_bpartner_id, ad_client_id, ad_org_id, isactive, created, createdby, updated, updatedby, iscurrentvendor, c_uom_id, c_currency_id, pricelist, pricepo, priceeffective, pricelastpo, pricelastinv, vendorproductno, upc, vendorcategory, discontinued, discontinuedby, order_min, order_pack, costperorder, deliverytime_promised, deliverytime_actual, qualityrating, royaltyamt, manufacturer ) 
    select m.m_product_id, m.c_bpartner_id, m.ad_client_id, m.ad_org_id, m.isactive, m.created, m.createdby, m.updated, m.updatedby, m.iscurrentvendor, m.c_uom_id, m.c_currency_id, m.pricelist, m.pricepo, m.priceeffective, m.pricelastpo, m.pricelastinv, m.vendorproductno, m.upc, m.vendorcategory, m.discontinued, m.discontinuedby, m.order_min, m.order_pack, m.costperorder, m.deliverytime_promised, m.deliverytime_actual, m.qualityrating, m.royaltyamt, m.manufacturer from mierp001.m_product_po AS m
     WHERE m.ad_client_id=1000000 and m_product_id not in
(139
,140
,141
,142
,145
,130
,147
,148
,128
,1000000 )
"""
		done = doSql(sql)
		
		TABLENAME = "m_product" 
//		rows = n_live_tup[TABLENAME]
//		done = doInsert(TABLENAME)
//		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)

//		TABLENAME = "m_product_po"
//		rows = n_live_tup[TABLENAME]
//		done = doInsert(TABLENAME)
//		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
			
		TABLENAME = "m_pricelist_version"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "m_productprice"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"

		TABLENAME = "pp_product_bom"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "pp_product_bomline"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "r_category"
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
		
		TABLENAME = "r_group"  
		rows = n_live_tup[TABLENAME]
		done = doInsert(TABLENAME)
		println "${CLASSNAME}:run ${done} for table ${TABLENAME} rows=${rows}.\n"
		done = updateSequence(TABLENAME)
				
		return null;
	}

  // wird in eclipse benötigt, damit ein "Run As Groovy Script" möglich ist (ohne Inhalt)
  // nach dem Instanzieren wird run() ausgeführt
  static main(args) {
  }

}
