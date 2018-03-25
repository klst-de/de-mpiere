package com.klst.opentrans.process;

import java.io.InputStream;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.adempiere.exceptions.AdempiereException;
import org.compiere.model.MInOut;
import org.compiere.model.MInOutLine;
import org.compiere.model.MOrderLine;
import org.compiere.model.MUOM;
import org.compiere.process.DocAction;
import org.opentrans.xmlschema._2.DISPATCHNOTIFICATION;
import org.opentrans.xmlschema._2.DISPATCHNOTIFICATIONITEM;
import org.opentrans.xmlschema._2.OPENTRANS;
import org.opentrans.xmlschema._2.ORDERREFERENCE;
import org.opentrans.xmlschema._2.PRODUCTID;
import org.w3c.dom.Document;

import com.klst.opentrans.MOrder;
import com.klst.opentrans.MUoM;
import com.klst.opentrans.DATETIME;
//import com.klst.opentrans.XmlReader; // wird durch Transformer ersetzt
import com.klst.opentrans.Transformer;

/*
 * wird nicht von SvrProcess abgeleitet, da mit CreateProductProcess vieles gemeinsam genutzt wird
 * 
 * prepare() und doIt() aus super!
 */
public class DispatchNotificationProcess extends CreateProductProcess {
	
	private DISPATCHNOTIFICATION unmarshalAvis(String uri) {
//		XmlReader reader = getXmlReader();
//		DISPATCHNOTIFICATION avis = null;
//		try {
//			InputStream is = new AvisPipedInputStream(uri);
//			Document doc = reader.parseDocument(is);
//			avis = (DISPATCHNOTIFICATION)reader.unmarshal(doc, DISPATCHNOTIFICATION.class);
//		} catch (Exception e) {
//			log.warning(e.getMessage());
//			throw new AdempiereException("NO opentrans-DISPATCHNOTIFICATION in "+uri );
//		}
//		return avis;
		
		DISPATCHNOTIFICATION avis = null;
		OPENTRANS ot;
		try {
			InputStream is = new AvisPipedInputStream(uri);
			ot = transformer.toModel(is);
			avis = ot.getDISPATCHNOTIFICATION();
		} catch (Exception e) {
			log.warning(e.getMessage());
			throw new AdempiereException("NO opentrans-DISPATCHNOTIFICATION in "+uri );
		}
		return avis;
	}
	
	/*
	 * Perform process (aus super) verarbeitet mehrere files und ruft doOne für ein opentrans-ORDER
	 * 
	 * @return Message
	 * @throws Exception
	 */	
	@Override
	protected String doOne(String msg, String uri) throws Exception {
		
		String ret = uri;
		DISPATCHNOTIFICATION avis = unmarshalAvis(uri);
		
		try {
			/*
			 * suchen nach der entsprechenden Order / OrderItem (es muss genau eine sein):
			 *  ? muss dieser "Fertiggestellt" sein? ja
			 *  ==> also fertgstellen
			 * 
	<DISPATCHNOTIFICATION_ITEM_LIST>
		<DISPATCHNOTIFICATION_ITEM>
			<LINE_ITEM_ID>1.0</LINE_ITEM_ID>
...			
			<ORDER_REFERENCE>
				<ORDER_ID>8662</ORDER_ID>
				<LINE_ITEM_ID>1</LINE_ITEM_ID>
			</ORDER_REFERENCE>
...
		</DISPATCHNOTIFICATION_ITEM>
			 * 
			 * erstellen Lieferschein aus order
			 */
			Map<String,MOrder> mOrder = processItems(avis);
			log.info("#mOrder="+mOrder.size() );
			if(mOrder.size() != 1)
				throw new IllegalArgumentException("Avis refers to "+mOrder.size()+" Order(s). Shold be one.");
			
			MOrder order = mOrder.values().iterator().next();
			MOrderLine[] oLines = order.getLines(true, "M_Product_ID"); // boolean requery
			log.info(" order="+order 
					+" oLines="+oLines.length
					);
			// fertgstellen
			order.processIt(DocAction.ACTION_Complete);
			if(DocAction.ACTION_Complete.equals(order.getDocStatus())) {
				log.info("order.DocStatus="+order.getDocStatus() );
				order.saveEx();
			}
			
			// erstellen Lieferschein aus order
			Timestamp movementDate = this.getNotificationDate();
			MInOut lieferschein = this.createFrom(order, movementDate);
			
			/* map: Line(IntegerString) --> oLine(MOrderLine)
			 * mOrderLine initial befüllt mit oLines/Auftragzeilen des zugehörigen Auftrags
			 */
			Map<String,MOrderLine> mOrderLine = new HashMap<String,MOrderLine>();
			// loop - orderLines, befüllen mOrderLine
			for (int i = 0; i < oLines.length; i++) {
				MOrderLine oLine = oLines[i];
				String key = Integer.toString(oLine.getLine());
				mOrderLine.put(key, oLine);
			}
			/*
			 * in opentrans-2.0.jar gibt es kein 'ARTICLE_ID', daher:
[Error] :57:16: cvc-complex-type.2.4.a: Invalid content was found starting with element 'ARTICLE_ID'. One of '{"http://www.opentrans.org/XMLSchema/2.0fd":PRODUCT_ID}' is expected.
			 * 
			 * zwei Listen avisItems und avisItemsDone:
			 * avisItems initial befüllt mit Aviszeilen
			 * avisItemsDone initial leer
			 */
			List<DISPATCHNOTIFICATIONITEM> avisItemsDone = new Vector<DISPATCHNOTIFICATIONITEM>();
			// loop - avisItems
			for(Iterator<DISPATCHNOTIFICATIONITEM> i = avisItems.iterator(); i.hasNext(); ) {
				DISPATCHNOTIFICATIONITEM item = i.next();
				ORDERREFERENCE orderRef = item.getORDERREFERENCE();
				PRODUCTID productId = item.getPRODUCTID();
				//MOrder order = mOrder.get(orderRef.getORDERID());
				String key = orderRef.getLINEITEMID();
				MOrderLine oLine = mOrderLine.get(key);
				MUoM mUoM = new MUoM(this.getCtx(), oLine.getC_UOM_ID(), this.get_TrxName());
				MUOM unit = MUoM.getOrCreate(this.getCtx(), item.getORDERUNIT(), this.get_TrxName());
				log.info(" ORDER_REFERENCE.LINEITEMID="+key 
						+" LINE_ITEM_ID="+item.getLINEITEMID()
						+" QUANTITY="+item.getQUANTITY() + "=?=" + oLine.getQtyEntered() + "(QtyEntered)"
						+" ORDERUNIT="+unit + "=?=" + mUoM
						+" SUPPLIERPID="+productId.getSUPPLIERPID().getValue() + "=?=" + oLine.getM_Product().getSKU()
						);
				
				if(oLine.getM_Product().getSKU().endsWith(productId.getSUPPLIERPID().getValue())) {
					// gleiches Produkt
					MInOutLine line = new MInOutLine(lieferschein);
					line.setOrderLine(oLine, 0, item.getQUANTITY()); // @param locator=0, @param Qty used only to find suitable locator
					
					// wg. org.adempiere.exceptions.FillMandatoryException: Erforderliche Felder ausfüllen:  Bewegungs-Menge
//			        at org.compiere.model.MInOutLine.beforeSave(MInOutLine.java:531)					
					line.setQty(item.getQUANTITY());
					
					line.saveEx(this.get_TrxName());
					
					if(item.getQUANTITY().compareTo(oLine.getQtyEntered()) == 0 ) {
						// Menge wie bestellt
						avisItemsDone.add(item);
					} else {
						log.info("diff item " + key + ":(QtyEntered/Dispatched) " + oLine.getQtyEntered()+"/"+item.getQUANTITY() );
						ret = ret + " item " + key + ":(QtyEntered/Dispatched) " + oLine.getQtyEntered()+"/"+item.getQUANTITY();
					}
					
				} else {
					// Ersatzprodukt
					MInOutLine line = new MInOutLine(lieferschein);
					line.setOrderLine(oLine, 0, item.getQUANTITY());
					line.setQty(item.getQUANTITY());
					line.setC_UOM_ID(unit.get_ID());
					line.saveEx(this.get_TrxName());
					
					line.setDescription("ErsatzartikelNr="+productId.getSUPPLIERPID().getValue());
					log.info("diff item " + key + ":(productId/Qty) " + productId.getSUPPLIERPID().getValue()+"/"+item.getQUANTITY() );
					ret = ret + " item " + key + ":(productId/Qty) " + productId.getSUPPLIERPID().getValue()+"/"+item.getQUANTITY();				
				}
			}
			// am Ende muss avisItemsDone so gross wie avisItems sein ===> Belegstatus auf CO/Fertigstellen
			log.info("bearbeitet: #Items="+avisItemsDone.size() +"/"+ avisItems.size());
			if(avisItemsDone.size() == avisItems.size()) {
				lieferschein.processIt(MInOut.ACTION_Complete);
				lieferschein.saveEx(this.get_TrxName());
				ret = ret + " mapped to " + lieferschein;
			} else {
				ret = ret + " not completed " + lieferschein;
			}
			log.info("lieferschein.DocStatus="+lieferschein.getDocStatus());
			
		} catch (Exception e) {
			e.printStackTrace();
			log.warning(e.getMessage());
			throw new AdempiereException(e.getMessage() + " in "+uri );
		}
		
		return ret;
	}

	private DATETIME otDate = null;
	
	/*
	 * @param otDate , diverse Foramte: "09.09.2014" , "2009-05-13T06:20:00+01:00"
	 */
	private void setNotificationDate(String otDate) {
		this.otDate = new DATETIME(otDate);
	}
	private Timestamp getNotificationDate() {
		return this.otDate.getTimestamp();
	}

	List<DISPATCHNOTIFICATIONITEM> avisItems = null;
	/*
	 * liefert map otORDERID-String -> MOrder-Objekt
	 */
	private Map<String,MOrder> processItems(DISPATCHNOTIFICATION avis) {
		Map<String,MOrder> mOrder = new HashMap<String,MOrder>();
		this.avisItems = avis.getDISPATCHNOTIFICATIONITEMLIST().getDISPATCHNOTIFICATIONITEM();
		log.info("Avis Date="+avis.getDISPATCHNOTIFICATIONHEADER().getDISPATCHNOTIFICATIONINFO().getDISPATCHNOTIFICATIONDATE()
				+" #Items="+avisItems.size()
				);
		setNotificationDate(avis.getDISPATCHNOTIFICATIONHEADER().getDISPATCHNOTIFICATIONINFO().getDISPATCHNOTIFICATIONDATE()); 
		for(Iterator<DISPATCHNOTIFICATIONITEM> i = avisItems.iterator(); i.hasNext(); ) {
			DISPATCHNOTIFICATIONITEM item = i.next();
			ORDERREFERENCE orderRef = item.getORDERREFERENCE();
			log.info("Ref ORDERID="+orderRef.getORDERID()
					+" LINEITEMID="+orderRef.getLINEITEMID()
					);
			mOrder.put(orderRef.getORDERID(), MOrder.find(this.getCtx(), orderRef.getORDERID(), pDropShipBPartner_ID, pSalesRep_ID, this.get_TrxName()));		
		}
		return mOrder;
	}
	
	private MInOut createFrom(MOrder order, Timestamp movementDate) {
		if (order == null)
			throw new IllegalArgumentException("No Order");
		
		MInOut deliveryNote = new MInOut(order, 0, movementDate);
		deliveryNote.setDocAction(MInOut.DOCACTION_Prepare);
		deliveryNote.saveEx(this.get_TrxName());
		log.info("new deliveryNote="+deliveryNote + " DocStatus="+deliveryNote.getDocStatus()); 
		return deliveryNote;
	}
	
}
