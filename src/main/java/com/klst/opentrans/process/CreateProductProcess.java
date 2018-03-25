package com.klst.opentrans.process;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;

import javax.inject.Singleton;

import org.adempiere.exceptions.AdempiereException;
import org.bmecat.bmecat._2005.DESCRIPTIONSHORT;
import org.compiere.model.MProductPO;
import org.compiere.model.MProductPrice;
import org.compiere.model.MUOM;
import org.compiere.process.ProcessInfoParameter;
import org.compiere.process.SvrProcess;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.opentrans.xmlschema._2.OPENTRANS;
import org.opentrans.xmlschema._2.ORDER;
import org.opentrans.xmlschema._2.ORDERITEM;
import org.opentrans.xmlschema._2.PRODUCTID;
import org.opentrans.xmlschema._2.PRODUCTPRICEFIX;
import org.opentrans.xmlschema._2.TAXDETAILSFIX;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import com.klst.opentrans.MOrder;
import com.klst.opentrans.MProduct;
import com.klst.opentrans.MUoM;
//import com.klst.opentrans.XmlReader; // wird durch Transformer ersetzt
import com.klst.opentrans.Transformer;

public class CreateProductProcess extends SvrProcess {

	private Properties m_ctx;
	
	// TODO getter für die Parameter, vorab geht es auch mit protected:
	protected String pDateipfad = null;
	protected String pDateipfadProcessed = null;
	protected int pDropShipBPartner_ID = -1;
	protected int pSalesRep_ID = -1;
	
	/*
	 * getParameterAsString für parameter, nicht notwendig in Idempiere
	 */
	protected static String getParameterAsString(Object p) {
		if(p==null)
			return null;
		return p.toString();
	}
	
	/**
	 * Prepare - e.g., get Parameters.
	 */
	@Override
	protected void prepare() {
		ProcessInfoParameter[] para = getParameter();
		log.info("para.length="+para.length);
		for (int i = 0; i < para.length; i++)
		{
			String name = para[i].getParameterName();			
			if (name.equals("Dateipfad")) {
				pDateipfad = getParameterAsString(para[i].getParameter());
			}
			else if (name.equals("DateipfadProcessed"))
				pDateipfadProcessed = getParameterAsString(para[i].getParameter());
			else if (name.equals("C_BPartner_ID")) {
				log.fine("para.length="+para.length + " i="+i + " name="+name + " " +para[i].getParameter() );
				pDropShipBPartner_ID = para[i].getParameterAsInt();
			}
			else if (name.equals("SalesRep_ID"))
				pSalesRep_ID = para[i].getParameterAsInt();
			else
				log.log(Level.SEVERE, "Unknown Parameter: " + name);
		}
		m_ctx = Env.getCtx();
		log.info(" pDateipfad="+pDateipfad
				+" pDateipfadProcessed="+pDateipfadProcessed
				+" pDropShipBPartner_ID="+pDropShipBPartner_ID
				+" pSalesRep_ID="+pSalesRep_ID
				);
	}

	/**
	 * Perform process.
	 * 
	 * @return Message
	 * @throws Exception
	 */
	@Override
	protected String doIt() throws Exception {
		
		String msg="";
		pstmtProduct = DB.prepareStatement(SQL_PRODUCT, get_TrxName());
		
		if(pDateipfad==null || pDateipfadProcessed==null) {
			return "parm error:"+" inPath="+pDateipfad+" , outPath="+pDateipfadProcessed;
		}
		try {
			transformer = new Transformer();

			File dir = new File(pDateipfad);
			File dirto = new File(pDateipfadProcessed);
			String[] files = dir.list(); // no filter
			if(files==null || files.length==0) {
				return "no files:"+" inPath="+pDateipfad+" , outPath="+pDateipfadProcessed;
			}
			if(!dirto.isDirectory()) {
				return "no dir:"+" outPath="+pDateipfadProcessed;
			}
			if(dir.getAbsolutePath().equals(dirto.getAbsolutePath())) {
				return "must be different:"+" inPath="+pDateipfad+" , outPath="+pDateipfadProcessed;
			}
			for(int i=0; i<files.length; i++) {
				String uri = dir.getAbsolutePath() + File.separator + files[i];
				File file = new File(uri);
				File fileto = new File(dirto.getAbsolutePath() + File.separator + files[i]);
				log.info( "use " + uri + " isFile="+file.isFile() + " fileto.exists="+fileto.exists());
				if(file.isFile()) {
					try {
						msg = msg + "<br/>" + doOne(msg, uri);
						boolean moved = this.movefile(file, fileto);
						log.warning("moved="+moved + " to "+fileto.getAbsolutePath());
					} catch (Exception e) {
						log.warning(e.getMessage());
						msg = msg + "<br/>" + e.getMessage();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			log.warning(e.getMessage());
			msg = msg + "<br/>" + e.getMessage();
		}
		return msg;
	}
	
	protected boolean movefile(File src, File tgt) {
		return src.renameTo(tgt);
	}
	
//	private static XmlReader reader = null;
//	
//	protected XmlReader getXmlReader() {
//		if(reader==null) {
//			reader = XmlReader.newInstance();
//		}
//		return reader;
//	}
	
	protected Transformer transformer; // ein Singleton
	
	/**
	 * mit unmarshal+doOne wird ein opentrans-XML-Dokument (ORDER) uri 
	 * in entsprechende ADempiere-Objekte konvertiert
	 * 
	 * Der Prozess besteht aus zwei Schritten:
	 *  1. XML-unmarshall lierfert opentrans-pojo ORDER-Objektnetz
	 *  2. mapping liefert ADempiere-Objekte, in doOne()
	 *  
	 * @see https://groups.google.com/forum/#!topic/idempiere/PDA5GU5kxGo
	 * 
	 * @param uri
	 * @return opentrans-pojo ORDER-Objektnetz
	 */
	protected ORDER unmarshal(String uri) {
//		XmlReader reader = getXmlReader();
//		ORDER order = null;
//		try {
//			Document doc = reader.read(uri);
//			Node o = reader.getOrder(doc);
//			order = (ORDER)reader.unmarshal(o, ORDER.class);
//		} catch (Exception e) {
//			log.warning(e.getMessage());
//			throw new AdempiereException("NO opentrans-ORDER in "+uri );
//		}
//		return order;
		ORDER order = null;
		OPENTRANS ot;
		try {
			File file = new File(uri);
			InputStream is = new FileInputStream(file);
			ot = transformer.toModel(is);
			order = ot.getORDER();
		} catch (FileNotFoundException e) {
			log.warning(e.getMessage());
			throw new AdempiereException("File Not Found: "+uri );
		} catch (Exception e) {
			log.warning(e.getMessage());
			throw new AdempiereException("NO opentrans-ORDER in "+uri );
		}
		return order;
	}
	
	protected String doOne(String msg, String uri) throws Exception {
		
		String ret = uri;
		ORDER order = unmarshal(uri);
		
		// map Products:
		try {
			List<ORDERITEM> otItems = order.getORDERITEMLIST().getORDERITEM();
			int newProducts = 0;
			for(Iterator<ORDERITEM> i=otItems.iterator(); i.hasNext(); ) {
				ORDERITEM item = i.next();
				if(createProductIfNew(item, pDropShipBPartner_ID))
					newProducts++;
			}
			if(newProducts>0)
				ret = ""+newProducts+ " new Product(s) in ORDERID="+order.getORDERHEADER().getORDERINFO().getORDERID();
		} catch (Exception e) {
			log.warning(e.getMessage());
			throw new AdempiereException(e.getMessage() + " in "+uri );
		}
		
		return ret;
	}

	private boolean createProductIfNew(ORDERITEM item, int dropShipBPartner_ID) {
		PRODUCTID otProduct = item.getPRODUCTID(); // mandatory
		String vendorProductNo = null;
		MProduct product = null;
		boolean newProduct = true;
		if(otProduct.getSUPPLIERPID()==null) {
			throw new AdempiereException("No SUPPLIERPID" + " in item "+item.getLINEITEMID() );
		} else {
			vendorProductNo = otProduct.getSUPPLIERPID().getValue();
		}
		String desc0 = null;
		List<DESCRIPTIONSHORT> descList = otProduct.getDESCRIPTIONSHORT();
		if(descList==null) {
			// exception ? : später, wenn es tatsächlich benötigt wird
		} else {
			if(descList.size()==0) {
				// exception ?
			} else {
				desc0 = descList.get(0).getValue();
			}
		}
		PRODUCTPRICEFIX otPrice = item.getPRODUCTPRICEFIX();
		
		List<MProduct> pl = getProduct(vendorProductNo, dropShipBPartner_ID);		
		if(pl.isEmpty()) {
			log.info("*new Product* VendorProductNo="+vendorProductNo
					+" desc0="+desc0
					);
			BigDecimal pricepp = null;
			BigDecimal tax = null;
			if(otPrice==null) {
				throw new AdempiereException("No PRODUCTPRICE" + " in item "+item.getLINEITEMID() );
			} else {
				// PRICE_QUANTITY berücksichtigen
				pricepp = (otPrice.getPRICEQUANTITY()==null || otPrice.getPRICEQUANTITY().signum()==0) ? otPrice.getPRICEAMOUNT() 
						: otPrice.getPRICEAMOUNT().divide(otPrice.getPRICEQUANTITY());
				
				List<TAXDETAILSFIX> taxes = otPrice.getTAXDETAILSFIX();
				for(Iterator<TAXDETAILSFIX> it = taxes.iterator(); it.hasNext(); ) {
					TAXDETAILSFIX taxfix = it.next();
					if("VAT".equals(taxfix.getTAXTYPE())) {
						tax = taxfix.getTAX();
					}
				}
			}
			
			product = new MProduct(this.getCtx(), 0, this.get_TrxName());
			
			product.setSKU("::"+vendorProductNo);
			if(desc0==null) {
				throw new AdempiereException("No DESCRIPTION" + " in item "+item.getLINEITEMID() );
			} else {
				product.setName(desc0);	
			}
			
			// wg. FEHLER: NULL-Wert in Spalte „m_product_category_id“ verletzt Not-Null-Constraint
			product.setM_Product_Category_ID(product.getDefaultProductCategory().getM_Product_Category_ID());
			
			// wg. FEHLER: NULL-Wert in Spalte „c_taxcategory_id“ verletzt Not-Null-Constraint
			if(tax==null) {
				product.setC_TaxCategory_ID(product.getDefaultTaxCategory().getC_TaxCategory_ID());
			} else {
				// Bsp in order_LS3_31234_8689_2014-10-17-.441.xml
				product.setC_TaxCategory_ID(product.getTaxCategory(tax).getC_TaxCategory_ID());
			}
			
			// wg. FEHLER: NULL-Wert in Spalte „c_uom_id“ verletzt Not-Null-Constraint
			MUOM unit = MUoM.getOrCreate(this.getCtx(), item.getORDERUNIT(), this.get_TrxName());
			product.setC_UOM_ID(unit.getC_UOM_ID());
			
			product.setIsDropShip(MOrder.ISDROPSHIP);
			product.setIsStocked(false); // alle SOE-Produkte werden als "nicht lagerhaltig" definiert 
			
			// mierp-Besonderheit:
			product.set_ValueOfColumnReturningBoolean(MProduct.COLUMNNAME_priceso, pricepp);
			product.set_ValueOfColumnReturningBoolean(MProduct.COLUMNNAME_vendor_id, dropShipBPartner_ID);
			product.saveEx(this.get_TrxName());
			
			// pPO: auf mierp darf es nur einen geben!
			MProductPO pPO = product.findOrCreateMProductPO(dropShipBPartner_ID, vendorProductNo);
			pPO.setPriceList(pricepp);
			pPO.setC_UOM_ID(unit.getC_UOM_ID());
			pPO.saveEx(this.get_TrxName());
			
			// MProductPrice erstellen (vorsichtshalber existierende beachten, siehe com.klst.minhoff.process.ImportProduct) 
			int plvID = product.getDefaultSOPriceListVersion().getM_PriceList_ID();
			log.info(" PRICEAMOUNT="+otPrice.getPRICEAMOUNT()
					+" PRICEQUANTITY="+otPrice.getPRICEQUANTITY()
					+" pricepp="+pricepp
					+" plvID="+plvID
					);
			MProductPrice price = MProductPrice.get(getCtx(), plvID, product.getM_Product_ID(), get_TrxName());
			if (price == null) {
				price = new MProductPrice(getCtx(), plvID, product.getM_Product_ID(), get_TrxName());
			}
			price.setPrices(pricepp, pricepp, pricepp);
			// auf mierp c_taxcategory_id in TABLE m_productprice 
			price.saveEx(this.get_TrxName());
			
			log.info("product="+product + " pPO="+pPO + " price="+price);
		} else {
			product = pl.get(0); 
			newProduct = false;
		}
		return newProduct;
	}
	
	private static final String SQL_PRODUCT_PO = "SELECT m_product_id FROM m_product_po"
			+ " WHERE isactive='Y' AND c_bpartner_id = ? ";
	private static final String SQL_PRODUCT = "SELECT m_product_id FROM m_product"
			+ " WHERE isactive='Y' AND sku like ? and m_product_id IN(" + SQL_PRODUCT_PO + ")";
	private PreparedStatement pstmtProduct; // sucht ein Produkt
	
	/* zum Finden hat man nur SUPPLIERPID / in SKU hinter '::' , Bsp PGI525BK::101748090 
	 * sollte genau ein Produkt liefern, oder nix. Bei mehr bleibt nur exception über
	 * 
	 * not unique! Product '337014401' result.size=2 in C:\proj\minhoff\input\order_LS3_31234_8659_2014-10-10-.013
	 */
	private List<MProduct> getProduct(String otProductSupplierPid, int dropShipBPartner_ID) {
		List<MProduct> resultList = null;
		ResultSet rs;
		int M_Product_ID = -1; 
		resultList = new ArrayList<MProduct>();
		try {
			pstmtProduct.setString(1, "%"+otProductSupplierPid);
			pstmtProduct.setInt(2, dropShipBPartner_ID);
			rs = pstmtProduct.executeQuery();
			log.info("loop through ResultSet sql " + SQL_PRODUCT);
			while (rs.next()) {
				M_Product_ID = rs.getInt(1);
				log.info(" M_Product_ID=" + M_Product_ID + " otProductSupplierPid=" + otProductSupplierPid);
				resultList.add(new MProduct(this.getCtx(), M_Product_ID, this.get_TrxName()));
			}
			if(resultList.size()>1) {
				// hier genuegt eine Warnung : den Fehler gibt es aber in CreateOrderProcess
//				throw new AdempiereException(" not unique! Product '" + otProductSupplierPid + "' result.size="+resultList.size());
				log.warning("not unique! Product '" + otProductSupplierPid + "' result.size="+resultList.size());
			}
			if(resultList.isEmpty()) {
				log.info(" not found! Product with otProductSupplierPid='" + otProductSupplierPid + "'");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return resultList;
	}
	
}
