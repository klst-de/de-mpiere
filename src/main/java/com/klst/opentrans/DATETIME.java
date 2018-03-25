package com.klst.opentrans;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.bmecat.bmecat._2005.TypeDATETIME;
import org.compiere.util.CLogger;

public class DATETIME extends TypeDATETIME {

	private static final CLogger log = CLogger.getCLogger(DATETIME.class);
	private static DatatypeFactory datatypeFactory = null;
	
	public DATETIME(String value) {
		super();
		this.setDATE(value);
	}
	
	public void setDATE(String value) {
		if(datatypeFactory==null) {
			try {
				datatypeFactory = DatatypeFactory.newInstance();
			} catch (DatatypeConfigurationException e) {
				e.printStackTrace();
			}
		}
		try {
			XMLGregorianCalendar xmlGregCal = datatypeFactory.newXMLGregorianCalendar(value);
			super.setDATE(xmlGregCal);
			return;
		} catch (IllegalArgumentException e) {
			log.warning(e.toString());
		}
		Date date = setDATE(value, "dd.MM.yyyy");
		if(date==null) setDATE(value, "dd.MMM.yyyy");
	}

	private Date setDATE(String value, String format) {
		SimpleDateFormat dmy = new SimpleDateFormat( format );
		Date date = null;
		try {
			date = dmy.parse(value);
			log.finest(" date="+date);
			GregorianCalendar gregCal = new GregorianCalendar();
			gregCal.setTime(date);
			XMLGregorianCalendar xmlGregCal = datatypeFactory.newXMLGregorianCalendar(gregCal);
			super.setDATE(xmlGregCal);
		} catch (ParseException e) {
			log.warning(e.toString() + " assumed format="+format);
		}
		return date;
	}
	
	public Timestamp getTimestamp() {
		GregorianCalendar gregCal = this.getDATE().toGregorianCalendar();
		Date dateObj = gregCal.getTime();
		return new Timestamp(dateObj.getTime());
	}
	
	// --------------------
	public static void main(String[] args) {
		DATETIME dt = new DATETIME("2009-05-13T06:20:00+01:00");
		log.info(" DATE="+dt.getDATE() + " Timestamp="+dt.getTimestamp());
		
		dt.setDATE("09.09.2014");
		log.info(" DATE="+dt.getDATE() + " Timestamp="+dt.getTimestamp());
	}

}
