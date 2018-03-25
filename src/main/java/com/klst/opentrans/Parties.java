package com.klst.opentrans;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bmecat.bmecat._2005.TypePARTYID;
import org.opentrans.xmlschema._2.PARTY;
import org.opentrans.xmlschema._2.PARTIES;

public class Parties extends PARTIES {
	
	/*
	 * PARTY_ROLE values:
    <xsd:enumeration value="buyer"/>
    <xsd:enumeration value="central_regulator"/>
    <xsd:enumeration value="customer"/>
    <xsd:enumeration value="deliverer"/>
    <xsd:enumeration value="delivery"/>
    <xsd:enumeration value="document_creator"/>
    <xsd:enumeration value="final_delivery"/>
    <xsd:enumeration value="intermediary"/>
    <xsd:enumeration value="invoice_issuer"/>
    <xsd:enumeration value="invoice_recipient"/>invoice_recipient
    <xsd:enumeration value="ipp_operator"/>
    <xsd:enumeration value="manufacturer"/>
    <xsd:enumeration value="marketplace"/>
    <xsd:enumeration value="payer"/>
    <xsd:enumeration value="remittee"/>
    <xsd:enumeration value="standardization_body"/>
    <xsd:enumeration value="supplier"/>
    <xsd:enumeration value="other"/>
	*/
	
	public static final String BUYER = "buyer";
	public static final String DELIVERY = "delivery";
	public static final String INVOICE_RECIPIENT = "invoice_recipient";
	public static final String SUPPLIER = "supplier";

	public Parties(PARTIES parties) {
		super();
		this.party = parties.getPARTY();
	}
	
	/*
	 * liefert aus den vielen PARTIES (BusinessPartner) diejenigen mit PARTY_ROLE partyrole
	 */
	public List<String> getIDvalue(String partyrole) {
		List<String> idValue = new ArrayList<String>();
		for( Iterator<PARTY> i=this.getPARTY().iterator(); i.hasNext(); ) {
			PARTY party = i.next();
			if(party.getPARTYROLE().contains(partyrole)) {
				for( Iterator<TypePARTYID> pi=party.getPARTYID().iterator(); pi.hasNext(); ) {
					idValue.add(pi.next().getValue());
				}
			}
		}
		return idValue;		
	}

}
