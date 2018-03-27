package com.klst.opentrans;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.adempiere.exceptions.AdempiereException;
import org.compiere.model.MPriceListVersion;
import org.compiere.model.MProductCategory;
import org.compiere.model.MProductPO;
import org.compiere.model.MTaxCategory;
import org.compiere.util.DB;
import org.compiere.util.Env;

/**
 * diese Klasse dient der Erzeugung neuer Produkte aus opentrans-Items
 * 
 */
/*
 * Achtung : auf mierp ist das Produkt-Preis-Datenmodell anders!
 * 
  ... unitsperpack numeric(10,0) NOT NULL DEFAULT 1,
 idempiere:
  discontinuedat timestamp without time zone,
  copyfrom character(1) DEFAULT NULL::bpchar,
  m_product_uu character varying(36) DEFAULT NULL::character varying,
  m_parttype_id numeric(10,0) DEFAULT NULL::numeric,
  iskanban character(1) NOT NULL DEFAULT 'N'::bpchar,
  ismanufactured character(1) NOT NULL DEFAULT 'N'::bpchar,
  isphantom character(1) DEFAULT 'N'::bpchar,
  isownbox character(1) NOT NULL DEFAULT 'N'::bpchar,
  CONSTRAINT m_product_pkey PRIMARY KEY (m_product_id), ...
  
 mierp: ...
  sku character varying(30),
  unitsperpack numeric(10,0) NOT NULL DEFAULT 1,   bis hierhin gleich?!
 mierp:
  isdiverse character(1) NOT NULL DEFAULT 'N'::bpchar,
  page_catalog character varying(10),
  m_priceunit_id numeric(10,0),
  vendor_name character varying(255) NOT NULL DEFAULT 'NOT_SET'::character varying,
  priceso numeric,
  pricepo numeric,
  vendor_id numeric(10,0),
  iscategoryproduct character(1) NOT NULL DEFAULT 'N'::bpchar,
  CONSTRAINT m_product_pkey PRIMARY KEY (m_product_id), ...
  
 * 
 * zum Mappen hat man oft nur SUPPLIERPID / in SKU hinter '::' PGI525BK::101748090 und DESCRIPTION_SHORT :
            <PRODUCT_ID>
               <bmecat:SUPPLIER_PID>111106802</bmecat:SUPPLIER_PID>
               <bmecat:BUYER_PID type="BZRNR"/>
               <bmecat:DESCRIPTION_SHORT>Soennecken Collegeblock DIN A5 kariert 70g/m² holzfrei Spiralbindung weiß 80 Bl.</bmecat:DESCRIPTION_SHORT>
            </PRODUCT_ID>
 */
public class MProduct extends org.compiere.model.MProduct {

	private static final long serialVersionUID = -5583599285660100390L;

	private static final String SQL_PRODUCT_CAT = "SELECT * FROM m_product_category"
			+ " WHERE isactive='Y' AND ad_client_id = ? AND ad_org_id IN( 0, ? ) AND isdefault='Y' ";
	private PreparedStatement pstmtProductCat; 
	
	private static final String SQL_TAX = "SELECT c_taxcategory_id FROM c_tax t"
			+ " WHERE isactive='Y' AND ad_client_id = ? AND ad_org_id IN( 0, ? ) AND rate = ? ";
	private static final String SQL_TAX_CAT = "SELECT * FROM c_taxcategory c"
			+ " WHERE isactive='Y' AND c_taxcategory_id IN("+ SQL_TAX +")";
	private PreparedStatement pstmtTaxCat; 
	
	private static final String SQL_TAX_CAT_DEFAULT = "SELECT * FROM c_taxcategory"
			+ " WHERE isactive='Y' AND ad_client_id = ? AND ad_org_id IN( 0, ? ) AND isdefault='Y' ";
	private PreparedStatement pstmtTaxDefalutCat; 
	
	private static final String SQL_SOPRICELISTVERSION = "SELECT * FROM m_pricelist_version"
			+ " WHERE isactive='Y' AND ad_client_id = ? AND ad_org_id IN( 0, ? ) AND m_pricelist_id IN "
			+ "(select m_pricelist_id FROM m_pricelist WHERE isactive='Y' AND ad_client_id = ? AND ad_org_id IN( 0, ? ) AND issopricelist='Y') ";
	private PreparedStatement pstmtPricelistVersion;  
	
	// ctor
	public MProduct(Properties ctx, int M_Product_ID, String trxName) {
		super(ctx, M_Product_ID, trxName);
		pstmtProductCat = DB.prepareStatement(SQL_PRODUCT_CAT, trxName);
		pstmtTaxCat = DB.prepareStatement(SQL_TAX_CAT, trxName);
		pstmtTaxDefalutCat = DB.prepareStatement(SQL_TAX_CAT_DEFAULT, trxName);
		pstmtPricelistVersion = DB.prepareStatement(SQL_SOPRICELISTVERSION, trxName);
	}

	/* holt default (Standard) pcat
	 * wg FEHLER: NULL-Wert in Spalte „m_product_category_id“ verletzt Not-Null-Constraint
	 * 
	 * eigentlich sollte diese Methode static sein, aber wg pstmtProductCat und log geht es nicht
	 */
	public MProductCategory getDefaultProductCategory() {
		MProductCategory pcat = null;
		ResultSet rs;
		try {
			pstmtProductCat.setInt(1, Env.getAD_Client_ID(this.getCtx()));
			pstmtProductCat.setInt(2, Env.getAD_Org_ID(this.getCtx()));
			rs = pstmtProductCat.executeQuery();
			if(rs.next()) {
				pcat = new MProductCategory(this.getCtx(), rs, this.get_TrxName());
				log.info("pcat=" + pcat);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return pcat;
	}
	
	/* holt default taxcat
	 * wg. FEHLER: NULL-Wert in Spalte „c_taxcategory_id“ verletzt Not-Null-Constraint
	 * 
	 * eigentlich sollte diese Methode static sein, aber ...
	 */
	public MTaxCategory getDefaultTaxCategory() {
		MTaxCategory taxcat = null;
		ResultSet rs;
		try {
			pstmtTaxDefalutCat.setInt(1, Env.getAD_Client_ID(this.getCtx()));
			pstmtTaxDefalutCat.setInt(2, Env.getAD_Org_ID(this.getCtx()));
			rs = pstmtTaxDefalutCat.executeQuery();
			if(rs.next()) {
				taxcat = new MTaxCategory(this.getCtx(), rs, this.get_TrxName());
				log.info("taxcat=" + taxcat);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return taxcat;
	}
	
	public MTaxCategory getTaxCategory(BigDecimal rate) {
		MTaxCategory taxcat = null;
		ResultSet rs;
		try {
			pstmtTaxCat.setInt(1, Env.getAD_Client_ID(this.getCtx()));
			pstmtTaxCat.setInt(2, Env.getAD_Org_ID(this.getCtx()));
			pstmtTaxCat.setBigDecimal(3, rate);
			rs = pstmtTaxCat.executeQuery();
			if(rs.next()) {
				taxcat = new MTaxCategory(this.getCtx(), rs, this.get_TrxName());
				log.info("taxcat=" + taxcat + " rate=" + rate);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		if(taxcat==null) {
			throw new AdempiereException("No TaxCategory for rate=" + rate );
		}
		return taxcat;
	}
	
	/* holt default plv
	 * 
	 * eigentlich sollte diese Methode static sein, aber ...
	 */
	public MPriceListVersion getDefaultSOPriceListVersion() {
		MPriceListVersion plv = null;
		ResultSet rs;
		try {
			pstmtPricelistVersion.setInt(1, Env.getAD_Client_ID(this.getCtx()));
			pstmtPricelistVersion.setInt(2, Env.getAD_Org_ID(this.getCtx()));
			pstmtPricelistVersion.setInt(3, Env.getAD_Client_ID(this.getCtx()));
			pstmtPricelistVersion.setInt(4, Env.getAD_Org_ID(this.getCtx()));
			rs = pstmtPricelistVersion.executeQuery();
			if(rs.next()) {
				plv = new MPriceListVersion(this.getCtx(), rs, this.get_TrxName());
				log.info("plv="+plv);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return plv;
	}
	
	/*
	 * erstellt pPO, kein save!
	 */
	public MProductPO findOrCreateMProductPO(int dropShipBPartner_ID, String vendorProductNo) {
		MProductPO pPO = null;
		MProductPO[] pPOs = MProductPO.getOfProduct(this.getCtx(), this.getM_Product_ID(), this.get_TrxName());
		log.info("pPOs.length="+pPOs.length);
		if(pPOs.length==0) {
			return this.createMProductPO(dropShipBPartner_ID, vendorProductNo);	
		} else {
			for(int i=0; i<pPOs.length; i++) {
				log.info(" pPOs["+i+"]"+pPOs[i] + " C_BPartner_ID="+pPOs[i].getC_BPartner_ID()+ " VendorProductNo="+pPOs[i].getVendorProductNo());
				if(pPOs[i].getC_BPartner_ID()==dropShipBPartner_ID) {
					pPO = pPOs[i];
					pPO.setVendorProductNo(vendorProductNo);
//					return pPO;	// die anderen loggen!
				}
			}
			if(pPO==null)
				return this.createMProductPO(dropShipBPartner_ID, vendorProductNo);	
		}
		return pPO;	
	}
	
	/*
	 * erstellt neuen pPO, kein save!
	 */
	private MProductPO createMProductPO(int dropShipBPartner_ID, String vendorProductNo) {
		MProductPO pPO = new MProductPO(this.getCtx(), 0, this.get_TrxName());
		pPO.setM_Product_ID(this.getM_Product_ID());
		pPO.setC_BPartner_ID(dropShipBPartner_ID);
		pPO.setVendorProductNo(vendorProductNo);
		return pPO;	
	}

}
