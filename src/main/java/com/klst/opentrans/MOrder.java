package com.klst.opentrans;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.adempiere.exceptions.AdempiereException;
import org.compiere.model.MBPartner;
import org.compiere.model.MOrderLine;
import org.compiere.model.MOrgInfo;
import org.compiere.model.MShipper;
import org.compiere.model.MUOM;
import org.compiere.process.DocAction;
import org.compiere.util.DB;
import org.compiere.util.Env;
import org.opentrans.xmlschema._2.ORDER;
import org.opentrans.xmlschema._2.ORDERHEADER;
import org.opentrans.xmlschema._2.ORDERINFO;
import org.opentrans.xmlschema._2.ORDERITEM;
import org.opentrans.xmlschema._2.ORDERSUMMARY;
import org.opentrans.xmlschema._2.PRODUCTID;
import org.opentrans.xmlschema._2.PRODUCTPRICEFIX;
import org.opentrans.xmlschema._2.REMARKS;

import com.klst.opentrans.DATETIME;
import com.klst.opentrans.Parties;

/*
 * MOrder-Mapping erweitert MOrder um opentrans-mapping Funktionalität,
 * die zentrale (static)-Methode ist mapping().
 * 
 * Neben den unique keys auf der Tabelle c_order:
 *  - CONSTRAINT c_order_pkey PRIMARY KEY (c_order_id)
 *  - UNIQUE INDEX c_order_documentno ... (documentno COLLATE pg_catalog."default", c_doctype_id, c_bpartner_id)
 *  - UNIQUE INDEX c_order_uu_idx ...     (c_order_uu COLLATE pg_catalog."default")
 * gibt es für die opentrans-Order noch den applikatorischen Ot-Key
 *  - (poreference, dropship_bpartner_id) für Order mit (IsActive='Y' AND isdropship ='Y')
 * und 
 *  poreference == otInfo.getORDERID()
 *  dropship_bpartner_id == dropShipBPartner_ID
 *  
 */
public class MOrder extends org.compiere.model.MOrder {

	private static final long serialVersionUID = 2890231590238517739L;
	
	private static final String SQL_OTORDER = "SELECT C_Order_ID FROM C_Order"
			+ " WHERE IsActive='Y' AND ad_client_id = ? AND ad_org_id IN( 0, ? )" // para 1,2
			+ " AND dropship_bpartner_id = ? "                                    // para 3
			+ " AND poreference = ? "                                             // para 4
//			+ " AND isdropship ='Y'"
			;
	private PreparedStatement pstmtOtOrder; // sucht den Auftrag
	private List<MOrder> existingOrders = null; // Suchergebnisse
	
	// isdefault nicht in AD39
	private static final String SQL_SHIPPER_DEFAULT = "SELECT * FROM m_shipper"
			+ " WHERE isactive='Y' AND ad_client_id = ? AND ad_org_id IN( 0, ? ) AND name like 'Standard%' ";
	private PreparedStatement pstmtDefalutShipper; 
	
	private static final String SQL_CUSTOMER = "SELECT c_bpartner_id FROM c_bpartner"
			+ " WHERE isactive='Y' AND iscustomer = 'Y' AND ( name = ? OR value = ? ) ";
	private PreparedStatement pstmtCustomer;  
	
	private static final String SQL_ORDERLINE = "SELECT * FROM C_OrderLine"
			+ " WHERE isactive='Y' AND ad_client_id = ? AND ad_org_id IN( 0, ? ) AND c_order_id = ? AND line = ?";
	private PreparedStatement pstmtOrderLine; 
	
	public static final boolean ISDROPSHIP = false; // wg. Validation failed - Auftrag null darf nicht als Streckengeschõft markiert werden
	
	private ORDERINFO otInfo = null;
	private String otSupplier = null; // i.A. "Minhoff GmbH"
	private String otByuer = null;
	private String otInvoice_recipient = null;
	private String otDelivery = null;
	private DATETIME otDateordered = null;
	
	private List<ORDERITEM> otItems = null;
	private ORDERSUMMARY otSummary = null;
	
	/**
	 * Default Constructor
	 * 
	 * @param ctx
	 * @param C_Order_ID
	 * @param trxName
	 */
	public MOrder(Properties ctx, int C_Order_ID, String trxName) {
		super(ctx, C_Order_ID, trxName);
		pstmtCustomer = DB.prepareStatement(SQL_CUSTOMER, trxName);
		pstmtOrderLine = DB.prepareStatement(SQL_ORDERLINE, trxName);
		pstmtDefalutShipper = DB.prepareStatement(SQL_SHIPPER_DEFAULT, trxName);
	}

	/*
	 * ctor mit OpentransOrderId
	 */
	public MOrder(Properties ctx, String trxName, String otId, int dropShipBPartner_ID) {
		super(ctx, 0, trxName);
		pstmtOtOrder = DB.prepareStatement(SQL_OTORDER, trxName);
		existingOrders = getExistingOrders(dropShipBPartner_ID, otId);
		if(existingOrders.isEmpty()) {
			log.warning("*** not found ot-Order *** , otId="+otId);
		}	
	}
	/*
	 * ctor mit OpentransOrder
	 */
	public MOrder(Properties ctx, String trxName, ORDER otOrder, int dropShipBPartner_ID) {		
		super(ctx, 0, trxName);
		this.initOpentransOrder(otOrder);
		pstmtOtOrder = DB.prepareStatement(SQL_OTORDER, trxName);
		pstmtCustomer = DB.prepareStatement(SQL_CUSTOMER, trxName);
		pstmtOrderLine = DB.prepareStatement(SQL_ORDERLINE, trxName);
		pstmtDefalutShipper = DB.prepareStatement(SQL_SHIPPER_DEFAULT, trxName);
		existingOrders = getExistingOrders(dropShipBPartner_ID, otInfo.getORDERID());
		if(!existingOrders.isEmpty()) {
			log.warning("*** found ot-Orders *** , #Orders="+existingOrders.size());
		}	
		log.info("ctor-ENDE C_Order_ID="+this.getC_Order_ID());
	}
	
	private void initOpentransOrder(ORDER otOrder) {
		log.info(" C_Order_ID="+this.getC_Order_ID());
		ORDERHEADER otHeader = otOrder.getORDERHEADER(); // ich benoetige nur ORDERINFO
		this.otInfo = otHeader.getORDERINFO();
		this.otItems = otOrder.getORDERITEMLIST().getORDERITEM();
		this.otSummary = otOrder.getORDERSUMMARY();
		
		String otID = otInfo.getORDERID();
		String otDate = otInfo.getORDERDATE(); // diverse Foramte: "09.09.2014" , "2009-05-13T06:20:00+01:00"
		log.info(" ORDERID="+otID
				+" ORDERDATE="+otDate
				+" CURRENCY="+otInfo.getCURRENCY()
		);
		this.otDateordered = new DATETIME(otDate);
		Parties otParties = new Parties(otInfo.getPARTIES());
		try {
			this.otSupplier = otParties.getIDvalue(Parties.SUPPLIER).get(0);
			this.otByuer = otParties.getIDvalue(Parties.BUYER).get(0);
			this.otInvoice_recipient = otParties.getIDvalue(Parties.INVOICE_RECIPIENT).get(0);
			this.otDelivery = otParties.getIDvalue(Parties.DELIVERY).get(0);
			log.info(" otSupplier='"+otSupplier + "' otByuer='"+otByuer 
					+ "' otInvoice_recipient='"+otInvoice_recipient + "' otDelivery='"+otDelivery+"'");
		} catch (IndexOutOfBoundsException e) {
			e.printStackTrace();
		}
		
		String deliverynote = null;
		List<REMARKS> remarks = otInfo.getREMARKS(); // mit
		for(Iterator<REMARKS> i=remarks.iterator(); i.hasNext(); ) {
			REMARKS remark = i.next();
			if("deliverynote".equals(remark.getType())) {
				deliverynote = getDeliverynote(remark.getValue());
			}
		}
		// otByuer/otInvoice_recipient anpassen wenn == 'Minhoff GmbH' / 'Minhoff GmbH'
		if(otByuer.equalsIgnoreCase(otSupplier)) { 
			otByuer = deliverynote==null ? otDelivery : deliverynote;
		}
		if(otInvoice_recipient.equalsIgnoreCase(otSupplier)) { 
			otInvoice_recipient = deliverynote==null ? otDelivery : deliverynote;
		}
		this.setDocStatus(DOCSTATUS_Drafted);
	}

	/*
	 * liefert das erste wort aus REMARKS/deliverynote, dieses enthält nach Vereinbarung otByuer bzw. otInvoice_recipient
	 * Bsp.: <REMARKS type="deliverynote">41684 </REMARKS>
	 * bzw. mail von Zubehoer@minhoff.de vom 7 Nov 2014 10:25:41 "Bestellung 8758" :
	 *       <REMARKS type="deliverynote">68000 BstNr 20779 </REMARKS>
	 * 
	 * @param inp
	 * @return String otByuer bzw. otInvoice_recipient oder null
	 */
	private String getDeliverynote(String inp) {
		Pattern pattern = Pattern.compile("(\\w+).*");
		Matcher matcher = pattern.matcher(inp);
		String res = null;
		if(matcher.matches()) {
			log.info("Deliverynote=" + inp);
			res = inp.substring(0, matcher.end(1));
		}
		return res;
	}

	private int getOrderLineID(MOrder order, int Line) {
		MOrderLine orderLine = null;
		int C_OrderLine_ID = 0;
		ResultSet rs;
		try {
			pstmtOrderLine.setInt(1, Env.getAD_Client_ID(order.getCtx()));
			pstmtOrderLine.setInt(2, Env.getAD_Org_ID(order.getCtx()));
			pstmtOrderLine.setInt(3, order.get_ID());
			pstmtOrderLine.setInt(4, Line);
			rs = pstmtOrderLine.executeQuery();
			if(rs.next()) {
				orderLine = new MOrderLine(order.getCtx(), rs, order.get_TrxName()); 
				C_OrderLine_ID = orderLine.get_ID();
			}
			log.info("orderLine=" + orderLine);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		return orderLine;
		return C_OrderLine_ID;
	}
			
	private void mapOtItems(int dropShipBPartner_ID) {
		log.info("OT #Items="+this.otItems.size()
				+" Summary="+this.otSummary.getTOTALITEMNUM()
				+" AD #Lines="+this.getLines().length
				+" TotalLines="+this.getTotalLines()
				+" Env.getAD_Org_ID(this.getCtx())="+Env.getAD_Org_ID(this.getCtx())
				);
		for(Iterator<ORDERITEM> i=otItems.iterator(); i.hasNext(); ) {
			ORDERITEM item = i.next();
			MUOM unit = MUoM.get(this.getCtx(), item.getORDERUNIT(), this.get_TrxName());
			log.info("\n item #="+item.getLINEITEMID() // String!
					+" QUANTITY="+item.getQUANTITY()
					+" ORDERUNIT="+item.getORDERUNIT() + " UoM="+unit
					);
			PRODUCTID otProduct = item.getPRODUCTID();
			String otProductSupplierPid = null;
			if(otProduct==null) {
				// nix
			} else {
				otProductSupplierPid = otProduct.getSUPPLIERPID()==null ? "null" : otProduct.getSUPPLIERPID().getValue();
				log.info(" SUPPLIERPID="+otProductSupplierPid
						+" DESCRIPTIONSHORT.size="+ (otProduct.getDESCRIPTIONSHORT()==null ? "null" : otProduct.getDESCRIPTIONSHORT().size())
						);
			}
			PRODUCTPRICEFIX otPrice = item.getPRODUCTPRICEFIX();
			if(otPrice==null) {
				// nix
			} else {
				log.info(" PRICEAMOUNT="+otPrice.getPRICEAMOUNT()
						+" PRICEQUANTITY="+otPrice.getPRICEQUANTITY()
						+" TAXDETAILSFIX.size="+ (otPrice.getTAXDETAILSFIX()==null ? "null" : otPrice.getTAXDETAILSFIX().size())
						);
			}
			
			int orderLineID = getOrderLineID(this, Integer.parseInt(item.getLINEITEMID())); 
			MOrderItem oi = new MOrderItem(this, item, orderLineID, dropShipBPartner_ID);
			MProduct p = oi.mapProduct();
			
			oi.setQty(item.getQUANTITY());	
			
			log.info(" QtyEntered="+oi.getQtyEntered() + " QtyOrdered="+oi.getQtyOrdered() + " PriceList_ID="+this.getM_PriceList_ID());
			
			log.info(" Line UOM="+oi.getC_UOM_ID()
					+" order.Precision="+this.getPrecision()
					+" order.PriceList="+this.getM_PriceList_ID()
					+" order.isSOTrx="+this.isSOTrx()
					);
//			oi.setPrice(otPrice.getPRICEAMOUNT()); // Use this Method if the Line UOM is the Product UOM 
			
//			oi.setPriceActual(otPrice.getPRICEAMOUNT()); // (actual price is not updateable)
//			oi.setTax();
			
			oi.saveEx(this.get_TrxName());
		}
	}

	/* mapping Headerdata und
	 *  - Delivery / Lieferung
	 *  - Invoicing / Rechnungsstellung
	 *  - Status  
	 * 
	 * @param dropShipBPartner_ID
	 * @param SalesRep_ID
	 * @return customers , i.A. == 1 , negativ == not saved
	 */
	private int mapping(int dropShipBPartner_ID, int SalesRep_ID) {
		// Headerdata
		int customers = this.setCustomer(SalesRep_ID);

		this.setPOReference(this.otInfo.getORDERID());
		this.setDateOrdered(this.otDateordered.getTimestamp());
		// wg Idee von WIL hier openTRANS eintragen:
		this.setC_OrderSource_ID(1000000);
		
		// Delivery:
		// wg. Validation failed - Auftrag null darf nicht als Streckengeschõft markiert werden
		this.setM_Warehouse_ID(this.getDropShip_Warehouse_ID());
		this.setIsDropShip(ISDROPSHIP);
		this.setDropShip_BPartner_ID(dropShipBPartner_ID);	
		this.setDeliveryRule(DELIVERYRULE_Manual); // "Liefersrt" Manual
		this.setDeliveryViaRule(DELIVERYVIARULE_Shipper); // "Lieferung durch S=Shipper/Standard - Frei Haus, D=Delivery/Versand"
		this.setM_Shipper_ID(this.getDefaultShipper().get_ID());
		
		// Invoicing:
		this.setInvoiceRule(INVOICERULE_AfterDelivery);
		this.setSalesRep_ID(SalesRep_ID); 
		
		log.info(" BPartner.PriceList=" + ( this.getC_BPartner()==null ? "BPartner==null" : this.getC_BPartner().getPO_PriceList() ) 
				+" M_PriceList="+this.getM_PriceList()
				);
		// TODO: getDefaultSOPriceListVersion kann man hier nicht verwenden ---- const ? 1000000
		this.setM_PriceList_ID(1000000);
		
		if(save()) {
			log.info(" saved " + this + " DocStatus="+this.getDocStatus());
		} else {
			log.warning(" *not saved* " + this );
			customers = 0 - customers; // negativ!
		}
		return customers;
	}
	
	/*
	 * die DropShip_Warehouse_ID wird im MOrgInfo-Objekt abgelegt
	 */
	private int getDropShip_Warehouse_ID() {
		return MOrgInfo.get(this.getCtx(), Env.getAD_Org_ID(this.getCtx()), this.get_TrxName()).getDropShip_Warehouse_ID();
	}
	
	/* holt default shipper
	 * 
	 * auf mierp ist es "Standard - Frei Haus"
	 */
	public MShipper getDefaultShipper() {
		MShipper shipper = null;
		ResultSet rs;
		try {
			pstmtDefalutShipper.setInt(1, Env.getAD_Client_ID(this.getCtx()));
			pstmtDefalutShipper.setInt(2, Env.getAD_Org_ID(this.getCtx()));
			rs = pstmtDefalutShipper.executeQuery();
			if(rs.next()) {
				shipper = new MShipper(this.getCtx(), rs, this.get_TrxName());
				log.info("shipper=" + shipper);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return shipper;
	}
	
	/* map otByuer nach C_BPartner_ID, wenn nicht möglich: otDelivery, altByuer
	 * 
	 * @param altByuer
	 * @return customers , i.A. == 1
	 */
	private int setCustomer(int altByuer) {
		int customers = 1;
		log.info("this="+this
				+" new="+this.is_new()
				+" DocStatus="+this.getDocStatus()
				+" C_BPartner_ID="+this.getC_BPartner_ID() 
				+" otByuer="+this.otByuer
				+" otDelivery="+this.otDelivery
				);

		if(this.is_new()) {
			List<MBPartner> customerList = getCustomer(this.otByuer);
			customers = customerList.size();
			if(customers==1) {
				this.setBPartner(customerList.get(0));
				return customers;
			}
			
			customerList = getCustomer(this.otDelivery);
			customers = customerList.size();
			if(customers==1) {
				this.setBPartner(customerList.get(0));
				return customers;
			}
			
			// Hinweis für Kundenbetreuer in Beschreibung bei nichteindeutigen Customer
			this.setBPartner(new MBPartner(this.getCtx(), altByuer, this.get_TrxName()));
			this.setDescription("not unique customer with Name/Value='" + otByuer + "'" + " and '" + otDelivery + "'");
		}
		return customers;
	}
	
	/*
	 * sollte immer nur einen Kunden liefern
	 */
	private List<MBPartner> getCustomer(String otByuer) {
		List<MBPartner> resultList = null;
		ResultSet rs;
		try {
			pstmtCustomer.setString(1, otByuer);
			pstmtCustomer.setString(2, otByuer);
			rs = pstmtCustomer.executeQuery();
			log.info("loop through ResultSet sql " + SQL_CUSTOMER);
			resultList = new ArrayList<MBPartner>();
			while (rs.next()) {
				int C_BPartner_ID = rs.getInt(1);
				log.info(" C_BPartner_ID=" + C_BPartner_ID + " Name/Value=" + otByuer);
				resultList.add(new MBPartner(this.getCtx(), C_BPartner_ID, this.get_TrxName()));
			}
			if(resultList.size()!=1) {
				log.warning(" not unique! Customer with Name/Value='" + otByuer + "' result.size="+resultList.size());
			}
			if(resultList.isEmpty()) {
				log.warning(" not found! Customer with Name/Value='" + otByuer + "'");
//				throw new AdempiereException("not found! Customer with Name/Value='" + otByuer + "'");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return resultList;
	}
	
	/*
	 * normalerweise wird bei unmarshal ein neues MOrder-Objekt angelegt,
	 * es hat den eindeutigen applikatorischen Ot-Key (poreference, dropship_bpartner_id).
	 * 
	 * Hier wird geprüft, ob ein MOrder-Objekt mit dem Ot-Key bereits existiert.
	 * Diese Prüfung sollte also die leere Menge liefern!
	 */
	private List<MOrder> getExistingOrders(int dropShipBPartner_ID, String otID) {
		List<MOrder> resultList = null;
		ResultSet rs;
		try {
			pstmtOtOrder.setInt(1, Env.getAD_Client_ID(this.getCtx()));
			pstmtOtOrder.setInt(2, Env.getAD_Org_ID(this.getCtx()));
			pstmtOtOrder.setInt(3, dropShipBPartner_ID);
			pstmtOtOrder.setString(4, otID);
			rs = pstmtOtOrder.executeQuery();
			log.info("loop through ResultSet sql " + SQL_OTORDER);
			resultList = new ArrayList<MOrder>();
			while (rs.next()) {
				int C_Order_ID = rs.getInt(1);
				log.info(" C_Order_ID=" + C_Order_ID );
				resultList.add(new MOrder(this.getCtx(), C_Order_ID, this.get_TrxName()));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return resultList;
	}

	/*
	 * Statusübergang DOCSTATUS_Drafted => DOCSTATUS_InProgress
	 * 
	 * completeIt() => returns new status (Complete, In Progress, Invalid, Waiting ..)
	 * prepareIt() => returns new status (In Progress or Invalid) 
	 * 
	 * @see org.compiere.model.MOrder#processIt(String)
	 * Statusübergänge: @see org.compiere.process.DocumentEngine#getActionOptions()
	 * 
	 * @param complete - do complete
	 * @return DocStatus
	 */
	public String processIt(boolean complete) {
		int lines = this.getLines(true,null).length;
		log.info("complete="+complete+ " TotalLines="+Integer.valueOf(lines).longValue() +"=?="+ this.otSummary.getTOTALITEMNUM());
		if(Integer.valueOf(lines).longValue()==this.otSummary.getTOTALITEMNUM().longValue()) {
			if(super.processIt(DocAction.ACTION_Complete)) {
				log.info("done DocAction.ACTION_Complete status="+this.getDocStatus()); 
			} else {
				log.warning("Problem bei complete");
			}
		}
		if(DOCSTATUS_Drafted.equals(this.getDocStatus())) {
			// was ist schief gelaufen?
			log.warning(" *not saved* " + this );
			return this.getDocStatus();
		}
		if(DOCSTATUS_InProgress.equals(this.getDocStatus())) {
			this.setDocAction(DocAction.ACTION_Complete);
			this.saveEx(); // kein Unterschied zu saveEx(this.get_TrxName());
			// die Order landet in Status IT - kann manuell in
//			if(complete) { // 2-tes complete mit Ziel status = Complete
//				super.completeIt();
//				log.info("done completeIt() status="+this.getDocStatus()); 
////				if(super.processIt(DocAction.ACTION_Complete)) {
////					log.info("complete DocAction.ACTION_Complete status="+this.getDocStatus()); 
////				} else {
////					log.warning("Problem bei complete");
////				}
//				this.saveEx();
//			}
		}
		return this.getDocStatus();
	}
	
	public static MOrder find(Properties ctx, String otID, int dropShipBPartner_ID, int salesRep_ID, String trxName) {
		MOrder order = new MOrder(ctx, trxName, otID, dropShipBPartner_ID);
		if(order.existingOrders.size()>0) {
			order = order.existingOrders.get(0); 
		} else {
			// TODO exception ?
			order = null;
		}
		return order;
	}
	
	/**
	 * implementiert das mapping
	 * 
	 * @param ctx
	 * @param otOrder
	 * @param dropShipBPartner_ID
	 * @param salesRep_ID
	 * @param trxName
	 * @return
	 */
	public static MOrder mapping(Properties ctx, ORDER otOrder, int dropShipBPartner_ID, int salesRep_ID, String trxName) {
		
		/*
		 * zuerst wird ein neues MOrder-Mapping-Objekt angelegt, weil das der Standardfall ist.
		 * Gibt es bereits die Ot-Order, so wird mit diesem Objekt weitergearbeitet,
		 * denn es darf kein zweites mit Ot-Key geben. 
		 */
		MOrder order = new MOrder(ctx, trxName, otOrder, dropShipBPartner_ID);
		if(order.existingOrders.size()>0) {
			order = order.existingOrders.get(0); 
			order.initOpentransOrder(otOrder);
		}	
		// bis hierhin fand noch kein Mapping statt, nur DR-Belege werden gemappt:
		if(DOCSTATUS_Drafted.equals(order.getDocStatus())) {
			int customers = order.mapping(dropShipBPartner_ID, salesRep_ID); // mit save
			order.mapOtItems(dropShipBPartner_ID);
			order.processIt(customers==1 ? true : false);
			if(customers!=1) {
				throw new AdempiereException(""+customers+" customers! " + order.getDescription());
			}
		} else {
			throw new AdempiereException(" DOCSTATUS '" + order.getDocStatus() + "' not Drafted : "+order );
		}
		return order;
	}

}
