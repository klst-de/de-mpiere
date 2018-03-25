package com.klst.opentrans.process;

import org.adempiere.exceptions.AdempiereException;
import org.opentrans.xmlschema._2.ORDER;

import com.klst.opentrans.MOrder;

/*
 * wird nicht von SvrProcess abgeleitet, da mit CreateProductProcess vieles gemeinsam genutzt wird
 * 
 * prepare() und doIt() aus super!
 */
public class CreateOrderProcess extends CreateProductProcess {
	
	/*
	 * Perform process (aus super) verarbeitet mehrere files und ruft doOne für ein opentrans-ORDER
	 * 
	 * @return Message
	 * @throws Exception
	 */	
	@Override
	protected String doOne(String msg, String uri) throws Exception {
		
		String ret = uri;
		ORDER order = unmarshal(uri);
		
		try {
			MOrder morder = MOrder.mapping(this.getCtx(), order, pDropShipBPartner_ID, pSalesRep_ID, this.get_TrxName());
			ret = ret + " mapped to " + morder;
		} catch (Exception e) {
			e.printStackTrace();
			log.warning(e.getMessage());
			throw new AdempiereException(e.getMessage() + " in "+uri );
		}
		
		return ret;
	}

}
