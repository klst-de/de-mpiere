package com.klst.opentrans;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.adempiere.exceptions.AdempiereException;
import org.bmecat.bmecat._2005.DESCRIPTIONSHORT;
import org.compiere.model.CalloutOrder;
import org.compiere.model.GridTab;
import org.compiere.model.MOrderLine;
import org.compiere.model.MProductPO;
import org.compiere.model.MProductPrice;
import org.compiere.model.MUOM;
import org.compiere.util.DB;
import org.opentrans.xmlschema._2.ORDERITEM;
import org.opentrans.xmlschema._2.PRODUCTID;
import org.opentrans.xmlschema._2.PRODUCTPRICEFIX;
import org.opentrans.xmlschema._2.TAXDETAILSFIX;

public class MOrderItem extends MOrderLine {

	private static final long serialVersionUID = 1687138425318329740L;
	
	private static final String SQL_PRODUCT_PO = "SELECT m_product_id FROM m_product_po"
			+ " WHERE isactive='Y' AND c_bpartner_id = ? ";
	private static final String SQL_PRODUCT = "SELECT m_product_id FROM m_product"
			+ " WHERE isactive='Y' AND sku like ? and m_product_id IN(" + SQL_PRODUCT_PO + ")";
	private PreparedStatement pstmtProduct; // sucht ein Produkt
	
	ORDERITEM otItem = null;
	int dropShipBPartner_ID = -1;
	
	// ctor
	public MOrderItem(Properties ctx, int C_OrderLine_ID, String trxName) {
		super(ctx, C_OrderLine_ID, trxName);
		pstmtProduct = DB.prepareStatement(SQL_PRODUCT, trxName);
	}

	/**
	 * see MOrderLine.MOrderLine(MOrder)
	 * 
	 * @param order
	 * @param item
	 */
	public MOrderItem(MOrder order, ORDERITEM item, int C_OrderLine_ID, int dropShipBPartner_ID) {
		this(order.getCtx(), C_OrderLine_ID, order.get_TrxName());
		if(order.get_ID() == 0)
			throw new IllegalArgumentException("MOrder not saved");
		
		log.info(" Line="+this.getLine()
				+" C_Order_ID="+this.getC_Order_ID()
				+" Product="+this.getProduct()
				);
		if(C_OrderLine_ID==0) {
			setC_Order_ID(order.getC_Order_ID());	//	parent
			setOrder(order); // ist mehr als setHeaderInfo(order)
			setLine(Integer.parseInt(item.getLINEITEMID())); // Exception bei parseInt
		}
		this.otItem = item;
		this.dropShipBPartner_ID = dropShipBPartner_ID;
	}
	
	
	/*
	 * zum mappen hat man in otProduct nur SUPPLIERPID / ==> in SKU hinter '::' PGI525BK::101748090 
	 * und DESCRIPTION_SHORT : ==> name
	 * 
	 * den Rest muss man sich zusammensuchen
	 */
	public MProduct mapProduct() {
		MProduct product = null;
		PRODUCTID otProduct = this.otItem.getPRODUCTID(); // mandatory
		String vendorProductNo = null;
		if(otProduct.getSUPPLIERPID()==null) {
			throw new AdempiereException("No SUPPLIERPID" + " in item "+this.otItem.getLINEITEMID() );
		} else {
			vendorProductNo = otProduct.getSUPPLIERPID().getValue();
		}
		String desc0 = null;
		List<DESCRIPTIONSHORT> descList = otProduct.getDESCRIPTIONSHORT();
		if(descList==null) {
			// exception ? 
		} else {
			if(descList.size()==0) {
				// exception ?
			} else {
				desc0 = descList.get(0).getValue();
			}
		}
		PRODUCTPRICEFIX otPrice = this.otItem.getPRODUCTPRICEFIX();
		
		List<MProduct> pl = getProduct(vendorProductNo, this.dropShipBPartner_ID);	
		if(pl.size()!=1) {
			// == 0 : darf nicht sein, weil CreateProductProcess gelafen sein muss!
			//  > 1 : nicht eindeutig (könnte man noch korrigieren : TODO )
			throw new AdempiereException(" not unique! Product '" + vendorProductNo + "' result.size="+pl.size());
		}
		product = pl.get(0); 
		log.info("*VendorProductNo="+vendorProductNo
				+" product="+product
				+" desc0="+desc0
				);
		BigDecimal pricepp = null;
		BigDecimal tax = null;
		if(otPrice==null) {
			throw new AdempiereException("No PRODUCTPRICE" + " in item "+this.otItem.getLINEITEMID() );
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
		
		// bis hierhin wurde nix gemappt.
		if(desc0!=null) {
//			product.setName(desc0);	// keine Korrekturen
		}
		
		if(tax==null) {
			product.setC_TaxCategory_ID(product.getDefaultTaxCategory().getC_TaxCategory_ID());
		} else {
			// Bsp in order_LS3_31234_8689_2014-10-17-.441.xml
			product.setC_TaxCategory_ID(product.getTaxCategory(tax).getC_TaxCategory_ID());
		}

		MUOM unit = MUoM.getOrCreate(this.getCtx(), this.otItem.getORDERUNIT(), this.get_TrxName());
		// wg.  UOM can't be changed if the product has movements or costs
//		product.setC_UOM_ID(unit.getC_UOM_ID());
		
		product.setIsDropShip(MOrder.ISDROPSHIP);
		
		// mierp-Besonderheit:
		product.set_ValueOfColumnReturningBoolean(MProduct.COLUMNNAME_priceso, pricepp);
		product.set_ValueOfColumnReturningBoolean(MProduct.COLUMNNAME_vendor_id, dropShipBPartner_ID);
		product.saveEx(this.get_TrxName());
		
		
		// pPO: auf mierp darf es nur einen geben!
		MProductPO pPO = product.findOrCreateMProductPO(dropShipBPartner_ID, vendorProductNo);
		pPO.setC_UOM_ID(unit.getC_UOM_ID());
// TODO	pPO.setC_Currency_ID(C_Currency_ID);
		pPO.setPriceList(pricepp);
		pPO.setPriceLastInv(pricepp);
		pPO.setVendorProductNo(vendorProductNo);
		pPO.saveEx(this.get_TrxName());
		
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
		
		this.setProduct(product);
		this.setPrice(pricepp); // Use this Method if the Line UOM is the Product UOM 
		product.saveEx(this.get_TrxName());
		
		return product;
	}
		
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

	// @see https://portal.klst.com/redmine/issues/287
	// todo
//	public void calculatePriceActual() {
//		//Properties ctx, int WindowNo, GridTab mTab
//		int WindowNo = -1; // ???
//		GridTab mTab = null; // ??? 
//		CalloutOrder.calculatePriceActual(this.getCtx(), WindowNo, mTab);
//	}
	
}
