package com.klst.mierp.process;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import org.compiere.model.MBPartner;
import org.compiere.model.MOrder;
import org.compiere.model.MOrderLine;
import org.compiere.model.MOrgInfo;
import org.compiere.model.MProduct;
import org.compiere.model.MProductPO;
import org.compiere.model.MProductionBatch;
import org.compiere.model.MUser;
import org.compiere.process.DocAction;
import org.compiere.util.AdempiereUserError;
import org.compiere.util.Env;
import org.eevolution.model.MPPProductBOM;
import org.eevolution.model.MPPProductBOMLine;

/*
die Klasse ist als client Prozess und als Erweiterung des Standard-wf Process_Order gedacht
 * ein wf-Prozessknoten schickt keine Meldungen an die ui,
   lediglich die boolsche Information done, siehe MWFActivity::private boolean performWork(...)
   done==false führt zum STATE_Suspended und der wf kann nicht fortgesetzt werden
 * als client Prozess bekommt der Nutzer ein feedback
 
 Parameter: C_Order_ID Auftrag für den die notwendigen Bestellungen vorgenommen werden
            Die Bestellungen werden für alle Auftragspositionen, auch für BOM-Komponenten, erstellt
 
Einige Lieferanten wurden von den Bestellungen ausgenommen (excludedVendors)
 ==> "keine Bestellung für "+mProduct + " (Lagerware) Lieferant "+vendor
Für diese Lieferanten werden künftig Bestellungen mit Mindestmenge generiert,
dazu ist aber eine funktionierende Bestandsführung notwendig.

 */
public class OrderPOCreate extends SvrWfProcess {

	// aus SvrProcess:
	// protected CLogger			log = CLogger.getCLogger (getClass());
	
	private static final int ITDZ_ORDER = 1000001; // c_ordersource
	private static final int INFOTECH_BPartner_ID = 38333;
	private static final int ZEPPELIN_Warehouse_ID = 1000002;
	private static final int ZEPPELIN_Locator_ID = 1000002;
	private static final int OPD_Locator_ID = 1000003; // vorest! vll spendieren wir ein Ausliefer-Lagerort
	private static final int DOCBASETYPE_MPO_ID = 1000047; // DOCBASETYPE_ManufacturingPlannedOrder = "MPO";

	private MOrder mOrder;
	private Map<Integer, MOrder> bestellungen = null; // key vendor_id
	private List<Integer> excludedVendors = null; // vendor_id für die keine Bestellungen vorbereitet werden
	private List<Integer> excludedProducts = null; // product_id für die keine Bestellungen vorbereitet werden (initial leer)
	private List<MProductionBatch> lProductionBatch = null;

	// Parameter:
	protected int p_C_Order_ID = -1;
	protected boolean p_ProcessAnyStatus = false; // Mehrfachbestellungen vermeiden
	
	public OrderPOCreate() {
		super();
		this.bestellungen = new HashMap<Integer, MOrder>();
		this.excludedProducts = new ArrayList<Integer>();
		this.excludedVendors = new ArrayList<Integer>();		
		// TODO besser: Produkte sind auf Lager mit einem Mindestbestand : M_Replenish
		this.lProductionBatch = new ArrayList<MProductionBatch>();	
	}
	
	protected void excludedVendor(int vendor_id) {
		this.excludedVendors.add(vendor_id);  
	}
	
	private int m_explosion_level = 0;
	private Set<MPPProductBOMLine> bomSet = new HashSet<MPPProductBOMLine>(); 
	private int explosion(MPPProductBOM bom) throws AdempiereUserError {
		if(bom == null) return 0;
		
		MPPProductBOMLine[] bom_lines = bom.getLines(new Timestamp(System.currentTimeMillis()));
		for(MPPProductBOMLine bomline : bom_lines) {
			MProduct component = MProduct.get(getCtx(), bomline.getM_Product_ID());

			if(component.isBOM() && !component.isStocked()) { // recursion for intermediate products
				explosion(MPPProductBOM.getDefault(component, this.get_TrxName()));
			} 
			else if(MProduct.PRODUCTTYPE_Item.equals(component.getProductType()) ||
					MProduct.PRODUCTTYPE_Service.equals(component.getProductType())
					) { // nicht BOM 
				log.config("m_explosion_level="+m_explosion_level + " components="+bomSet.size() 
						+ " "+component.getProductType() + " component="+component
						+ (component.isStocked()   ? " isStocked"   : " notStocked")
						+ (component.isPurchased() ? " isPurchased" : " notPurchased")
						+ " bomline.getQty()="+bomline.getQty()
						);
				bomSet.add(bomline);  
			} else {
				throw new AdempiereUserError("PRODUCTTYPE="+component.getProductType());
			}			
		}
		
		return bomSet.size();
	}
	
	private MProductionBatch createProduction(MProduct mProduct, BigDecimal targetQty) {
		log.info("for mProduct:"+mProduct);
		MProductionBatch mProductionBatch = new MProductionBatch(getCtx(), 0, get_TrxName());
		
		mProductionBatch.setC_DocType_ID(DOCBASETYPE_MPO_ID); 
		mProductionBatch.setM_Product_ID(mProduct.getM_Product_ID());
		
		// liefert org.adempiere.exceptions.PeriodClosedException: Periode geschlossen Datum=2018-07-05 00:00:00.0, Basisbelegart=Arbeitsauftrag
		// wenn MovementDate in der Zukunft liegt:
		mProductionBatch.setMovementDate(this.mOrder.getDatePromised());
		
		mProductionBatch.setDescription(""+this.mOrder + " for "+this.mOrder.getC_BPartner());
		mProductionBatch.setM_Locator_ID(ZEPPELIN_Locator_ID);
		mProductionBatch.setTargetQty(targetQty);
		// initialWerte
		mProductionBatch.setQtyOrdered(Env.ZERO);
		mProductionBatch.setCountOrder(0);
		mProductionBatch.setQtyCompleted(Env.ZERO);
		
		mProductionBatch.save(get_TrxName());
		return mProductionBatch;
	}


	/**
	 *  Prepare - e.g., get Parameters.
	 */
	@Override
	protected void prepare() {
		log.info("Table_ID.Record_ID="+this.getTable_ID()+"."+this.getRecord_ID());
		log.finest("ctx="+this.getCtx());
		
		p_C_Order_ID = getParameterAsInt("C_Order_ID");
		p_ProcessAnyStatus = getParameterAsBoolean("ProcessAnyStatus");
		
		if(p_C_Order_ID > 0) {
			mOrder = new MOrder(this.getCtx(), p_C_Order_ID, this.get_TrxName());
		} else if(MOrder.Table_ID == this.getTable_ID()) {
			mOrder = new MOrder(this.getCtx(), this.getRecord_ID(), this.get_TrxName());
		}

	}

	/**
	 *  Perform process.
	 *  @return Message (variables are parsed)
	 *  @throws Exception if not successful e.g.
	 *  throw new AdempiereUserError ("@FillMandatory@  @C_BankAccount_ID@");
	 */
	@Override
	protected String doIt() throws Exception {
		log.info("mOrder="+mOrder + " DocStatus="+mOrder.getDocStatus()); 

		// als Process: mit catch : blaue Meldung im Process-Fenster
		//  in groovy : startProcess true
		// als Process: ohne catch : rote Meldung im Process-Fenster
		//  in groovy : startProcess false - aber wo ist die Meldung????
//		try {
			// some checks
			if("SO".equals(mOrder.getC_DocType().getDocSubTypeSO()) && mOrder.getC_OrderSource_ID()==ITDZ_ORDER) {
				// OK - continue
			} else {
				return raiseError("kein ITDZ Standardauftrag "+mOrder
						, "Belegart muss Standardauftrag, 'Auftragsquelle' muss ITDZorder sein, Belegstatus 'in Verarbeitung'");
			}
			// see https://github.com/adempiere/adempiere/issues/1649
			if(DocAction.STATUS_InProgress.equals(mOrder.getDocStatus()) 
					|| (DocAction.STATUS_Completed.equals(mOrder.getDocStatus()) && p_ProcessAnyStatus)) {
				// OK - continue 
			} else {
				return raiseError("Beleg "+mOrder+" ist bereits 'Fertiggestellt' / Status="+mOrder.getDocStatus()
						, "damit werden Mehrfachbestellungen vermieden"
						+ " - diese Funktinalität kann mit Flag 'unabhängig vom Auftragsstatus durchführen' aufgehoben werden");
			}
//		} catch(Exception e) {
//			return this.sendMsg(e.getMessage()); 
//		} 

		MOrderLine[] mOrderLines = mOrder.getLines();
		// die Fälle A..D sind kommuniziert via Mail am 23 Aug 2017 15:34:36 +0200
		// A : wird eingekauft, ist Dienstleistung oder Software (nicht lagerhaltig)
		// B : wird eingekauft und zwischengelagert 
		// C : nicht eingekauft, eigene DL /sw
		// D : BOM
		for(int i=0; i<mOrderLines.length; i++) {
			MOrderLine mOrderLine = mOrderLines[i];
			MProduct mProduct = mOrderLine.getProduct();
			MPPProductBOM bom = null;
			log.info("mOrderLine="+mOrderLine + " mProduct="+mProduct
					+ (mProduct.isStocked()   ? " isStocked"   : " notStocked")
					+ (mProduct.isPurchased() ? " isPurchased" : " notPurchased")
					+ (mProduct.isBOM()       ? " isBOM"       : " notBOM")
					+ (mProduct.isVerified()  ? " isVerified"  : " notVerified")
					);
			if(mProduct.isPurchased()) { // Fall A oder B
				MProductPO[] mProductPOs = MProductPO.getOfProduct(this.getCtx(), mProduct.getM_Product_ID(), this.get_TrxName());
				// auf mierp mProductPOs.length maximal 1 und > 0 applikatorisch: alle isPurchased mussen einen Lieferanten haben
				if(mProductPOs.length==1) {
					log.info("Fall A oder B: mOrderLine="+mOrderLine + " QtyOrdered="+mOrderLine.getQtyOrdered() + " SKU="+mProduct.getSKU()
							+ " vendor_id="+mProductPOs[0].getC_BPartner_ID());
					bestelle(mProduct,mOrderLine);
				} else {
					return raiseError("isPurchased must have a supplier (mierp one supplier)"
							, "check product vendor in "+mProduct);
				}
			} 
			else if(mProduct.isBOM()) { // Fall D : BOM
				if(mProduct.isVerified()) {
					// Get BOM with Default Logic (Product = BOM Product and BOM Value = Product Value)
					bom = MPPProductBOM.getDefault(mProduct, this.get_TrxName());
					log.info("bom="+bom);
					
					// m_explosion_level und bomSet wird für explosion-Method verwendet
					m_explosion_level = 0;
					bomSet = new HashSet<MPPProductBOMLine>(); 
					if(explosion(bom)>0) {
						
//						try {
//							MProductionBatch pb = createProduction(mProduct,mOrderLine.getQtyOrdered());
//							if(pb.processIt("CO")) { // Complete
//								setMsg("ProductionPlan fertiggestellt für Multiprodukt "+mProduct);
//							} else {
//								setMsg("ProductionPlan vorbereitet für Multiprodukt "+mProduct);
//								lProductionBatch.add( pb );
//							}
//							pb.save();
//						} catch(Exception e) {
//							return raiseError(e.getMessage(),"***"); 
//						}
						// der fehler mit MovementDate passiert nicht in pb, sondern in MProduction objekt
						MProductionBatch pb = createProduction(mProduct,mOrderLine.getQtyOrdered());
						//pb.setIsAutoProduction(false); // TODO was macht das? 
						log.config("status after prepare: "+pb.prepareIt());
						if( pb.completeIt().equals(DocAction.STATUS_Completed)) {
							pb.save();
							setMsg("ProductionPlan "+pb+" fertiggestellt für Multiprodukt "+mProduct);
						} else {
							log.config("status after complete: "+pb.getDocStatus());
							//return raiseError("Document not complete: " + pb.getDocStatus(),"check "+pb); 
							setMsg("ProductionPlan "+pb+" vorbereitet für Multiprodukt "+mProduct);
						}

						for(Iterator<MPPProductBOMLine> it = bomSet.iterator(); it.hasNext(); ) { 
							MPPProductBOMLine mBOMLine = it.next();
							MProduct mComponent = MProduct.get(getCtx(), mBOMLine.getM_Product_ID()); 
							MProductPO[] mProductPOs = MProductPO.getOfProduct(this.getCtx(), mComponent.getM_Product_ID(), this.get_TrxName());
							// auf mierp mProductPOs.length maximal 1 und > 0 applikatorisch: alle isPurchased muessen einen Lieferanten haben
							if(mProductPOs.length==1) {
								log.info("Fall D (BOM):mOrderLine="+mOrderLine + " QtyOrdered="+mOrderLine.getQtyOrdered() + " SKU="+mComponent.getSKU()
										+ " vendor_id="+mProductPOs[0].getC_BPartner_ID());
								bestelle(mComponent,mOrderLine,mBOMLine);
							} else {
								return raiseError("isPurchased must have a supplier (mierp one supplier)"
										, "check product vendor in component "+mComponent);
							}
						}
					} else {
						return raiseError("No BOM Lines"
								, "check BOM definition for "+mProduct);
					}
				} else {
					return raiseError("BOM not verified for "+mProduct
							, "use verify Button to fix the problem");
				}
			} else { // Fall C : nicht eingekauft, eigene DL /sw
				String m = "eigene DL/sw mProduct="+mProduct + " / "+mProduct.getM_Product_Category();
				setMsg(m);
			}
		}
		
		// anzeigen
		for(Iterator<Integer> il = bestellungen.keySet().iterator(); il.hasNext(); ) {
			Integer key = il.next(); // vendor ID als Integer
			MBPartner vendor = new MBPartner(getCtx(), key.intValue(), get_TrxName());
			MOrder mOrder= bestellungen.get(key);
			setMsg("Bestellung "+mOrder.getDocumentNo()+" vorbereitet für Lieferant "+vendor);
		}

		log.info(this.getMsg());
//		if(lProductionBatch.isEmpty()) {
//			// nix tun, da keine Produktion angelegt
//		} else {
//			lProductionBatch.forEach((prod) -> {
//				prod.prepareIt();
//				prod.completeIt();				
//			});		
//		}

		return this.sendMsg(this.getMsg());
	}
	
	// mProduct beim Lieferanten auf Bestellliste setzen
	private void bestelle(MProduct mProduct, MOrderLine mOrderLine) throws AdempiereUserError {
		bestelle(mProduct, mOrderLine, null);
	}
	private void bestelle(MProduct mProduct, MOrderLine mOrderLine, MPPProductBOMLine bomline) throws AdempiereUserError {
		BigDecimal qtyOrdered = bomline==null ? mOrderLine.getQtyOrdered() : mOrderLine.getQtyOrdered().multiply(bomline.getQty()); 
		// TODO bomline.isQtyPercentage()
		if(bomline!=null && bomline.isQtyPercentage()) {
			raiseError("does not work for QtyPercentage","hint: to be implemented");
		}
		MProductPO[] mProductPOs = MProductPO.getOfProduct(this.getCtx(), mProduct.getM_Product_ID(), this.get_TrxName());
		// auf mierp mProductPOs.length maximal 1 und > 0 applikatorisch: alle isPurchased müssen einen Lieferanten haben
		if(mProductPOs.length==1) {
			log.info("qtyOrdered="+qtyOrdered + " SKU="+mProduct.getSKU()
					+ " vendor_id="+mProductPOs[0].getC_BPartner_ID());
		} else {
			raiseError("isPurchased must have a supplier (mierp one supplier)","hint: check product vendor");
		}
		
		int vendor_id = mProductPOs[0].getC_BPartner_ID();
		if(excludedVendors.contains(vendor_id)) { // keine Bestellung für diesen Lieferanten
			MBPartner vendor = new MBPartner(getCtx(), vendor_id, get_TrxName());
			Integer M_Product_ID = new Integer(mProduct.getM_Product_ID());
			if(excludedProducts.contains(M_Product_ID)) {
				// vermeiden mehrfacher Ausgabe
			} else {
				setMsg("keine Bestellung für "+mProduct + " (Lagerware) Lieferant "+vendor);
				excludedProducts.add(M_Product_ID); 
			}
			return;
		}
		
		MOrder bestellung = bestellungen.get(new Integer(vendor_id));
		if(bestellung==null) {
			bestellung = createPOForVendor(vendor_id, this.mOrder);
			bestellungen.put(new Integer(vendor_id), bestellung);
		}

		MOrderLine poLine = new MOrderLine(bestellung);
		poLine.setLine( bomline==null ? mOrderLine.getLine() : mOrderLine.getLine()+bomline.getLine()/10 );
		poLine.setM_Product_ID(mProduct.getM_Product_ID(), true); // true ==> set also UOM
		poLine.setRef_OrderLine_ID(mOrderLine.getC_OrderLine_ID()); // Reference to corresponding Sales/Purchase Order
		poLine.setQtyOrdered(qtyOrdered);
		poLine.setQtyEntered(qtyOrdered);
		poLine.setPrice();  
		poLine.saveEx(this.get_TrxName());
		// setC_Charge_ID setM_AttributeSetInstance_ID setDescription setDatePromised : siehe OrderPOCreate.createPOFromSO 

		// setLink_OrderLine_ID : This field links a sales order line to the purchase order line that is generated from it.
		// also das Gegenstück zu Ref_OrderLine, bei BOM-Komponenten macht es keine Sinn, da 1:n
		//mOrderLine.setLink_OrderLine_ID(poLine.getC_OrderLine_ID());
		//mOrderLine.saveEx(this.get_TrxName());
		log.info("saved poLine="+poLine + " in "+bestellung);
	}

	// wie OrderPOCreate.createPOForVendor  
	private MOrder createPOForVendor(int C_BPartner_ID, MOrder so) {
		log.info("start C_BPartner_ID="+C_BPartner_ID);
		MOrder po = new MOrder (getCtx(), 0, get_TrxName());
		po.setClientOrg(so.getAD_Client_ID(), so.getAD_Org_ID());
		po.setLink_Order_ID(so.getC_Order_ID());
		po.setIsSOTrx(false);
		po.setC_DocTypeTarget_ID();
		// bei fehlender EK M_PriceList_ID gibt es "/ by zero"
//		po.setM_PriceList_ID(EK_PriceList_ID); 
		
		po.setC_OrderSource_ID(so.getC_OrderSource_ID());
		//po.setDescription("generiert via "+getClass()); // nein, denn das sieht man auf der jasper Bestellung
		po.setPOReference(so.getDocumentNo());
		po.setPriorityRule(so.getPriorityRule());
		po.setSalesRep_ID(so.getSalesRep_ID());
		//	Set Vendor
		MBPartner vendor = new MBPartner (getCtx(), C_BPartner_ID, get_TrxName());
		po.setBPartner(vendor);
		MUser[] contacts = vendor.getContacts(false); // reload=false
		for (int i = 0; i < contacts.length; i++) {
			log.info(contacts[i].getName() + " LastContact="+contacts[i].getLastContact() + " EMail="+contacts[i].getEMail());
			if(contacts[i].getName().startsWith("Zentrale")) {
				po.setAD_User_ID(contacts[i].getAD_User_ID());
				break;
			}
		}
		//  Warehouse / DropShip
		if(C_BPartner_ID==INFOTECH_BPartner_ID) { // Infotech bzw TODO andere DL
			po.setM_Warehouse_ID(getDefaultDropShip_Warehouse_ID());			
			po.setIsDropShip(true); // Drop Ship ... bei Infotech!
			po.setDropShip_BPartner_ID(so.getC_BPartner_ID());
			po.setDropShip_Location_ID(so.getC_BPartner_Location_ID());
			po.setDropShip_User_ID(so.getAD_User_ID());
		} else {
			po.setM_Warehouse_ID(ZEPPELIN_Warehouse_ID);
			po.setIsDropShip(false); 
		}
		//	References
		po.setRef_Order_ID(so.getC_Order_ID());  // Reference to corresponding Sales/Purchase Order
		po.setC_Activity_ID(so.getC_Activity_ID());
		po.setC_Campaign_ID(so.getC_Campaign_ID());
		po.setC_Project_ID(so.getC_Project_ID());
		po.setUser1_ID(so.getUser1_ID());
		po.setUser2_ID(so.getUser2_ID());
		po.setUser3_ID(so.getUser3_ID());
		po.setUser4_ID(so.getUser4_ID());
		
		po.setC_PaymentTerm_ID(vendor.getC_PaymentTerm_ID());  // Zahlhungskondition
		//
		po.saveEx(this.get_TrxName());
		log.info("po saved vendor="+vendor + " po="+po);
		return po;
	}
	
	// get default drop ship warehouse
	private int getDefaultDropShip_Warehouse_ID() {
		MOrgInfo orginfo = MOrgInfo.get(getCtx(), this.mOrder.getAD_Org_ID(), get_TrxName());
		if (orginfo.getDropShip_Warehouse_ID() == 0 ) {
			log.log(Level.SEVERE, "Must specify drop ship warehouse in org info.");
		}
		return orginfo.getDropShip_Warehouse_ID();
	}


}
