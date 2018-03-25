package com.klst.opentrans.process;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.compiere.util.CLogger;

/*
 * Der Versand des Lieferavis seitens So.Shop kann nur per Mail oder 
 * in das FTP-Verzeichnis (als DISPATCHNOTIFICATION-xml) erfolgen. Ein Parallelbetrieb ist nicht möglich.
 * 
 * Lieferavis wird zur Zeit ausschließlich in Opentrans-Version 1.0 bereitgestellt:
 *   <DISPATCHNOTIFICATION version="1.0">
 * Damit kann man mit opentrans-2.0.jar die Lieferavis nicht parsen.
 * 
 * Diese Klasse hat zwei Funktionalitäten:
 * 
 * 1. ermöglicht Workaround mit css.
 * 
 * 2. Workaround, liest Lieferavis die bereitgestellten Lieferavis V1.0
 * und liefert einen xml-Stream mit einem opentrans-2.0.jar DISPATCHNOTIFICATION node.
 * 
 * 2.1. Workaround, Umbenennung der Elemente von ARTICLE nach PRODUCT , ORDER_UNIT
 * und SUPPLIER_AID nach bmecat:SUPPLIER_PID
 * @see Kapitel 3.3 Erweiterungen in openTRANS® 2.0 in "opentrans_2_1_de - DISPATCHNOTIFICATION.pdf"
 */
public class AvisPipedInputStream extends PipedInputStream {

	private static final CLogger log = CLogger.getCLogger(AvisPipedInputStream.class);

	BufferedReader bReader = null;
	PipedOutputStream pipedSrc = null;
	
	/* @see https://portal.klst.com/redmine/issues/343
	 *  diese Paraameter wird für getBytes() verwendet
	 *  null : in AvisPrepareProcess , damit mit css unter w7 die Anzeige korrekt ist
	 *  UTF-8 : sonst
	 */	
	String charsetName = null;
	
	// ctor
	public AvisPipedInputStream(String uri) throws FileNotFoundException, IOException {
		this(uri, "UTF-8");		
	}
	public AvisPipedInputStream(String uri, String charsetName) throws FileNotFoundException, IOException {
		super();
		log.fine("ctor()" + " uri="+uri);
		bReader = new BufferedReader(new InputStreamReader(new FileInputStream(uri), Charset.forName("UTF-8")));
		pipedSrc = new PipedOutputStream();
		this.charsetName = charsetName;
		nextline();
		connect(pipedSrc); // throws IOException 
	}
	
	String line;
	byte[] bytes;
	int pos = 0;
	
	private static final String CSS = "<?xml-stylesheet type=\"text/css\" href=\"css/browseranzeige_soennecken.css\"?>";

	private static final String DISPATCHNOTIFICATION20 = "<DISPATCHNOTIFICATION version=\"2.0\" "
			+ "xmlns=\"http://www.opentrans.org/XMLSchema/2.0fd\" "
			+ "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" "
			+ "xsi:schemaLocation=\"http://www.opentrans.org/XMLSchema/2.0fd opentrans_2_0.xsd\" "
			+ "xmlns:bmecat=\"http://www.bmecat.org/bmecat/2005\" "
			+ "xmlns:mime=\"http://www.w3.org/2005/05/xmlmime\"> ";
	
	private String matcherArticlId(String inp) {
		Pattern pattern = Pattern.compile("(<|</)ARTICLE_ID>");
		Matcher matcher = pattern.matcher(inp);
		String res = null;
		if(matcher.matches()) {
			res = inp.substring(0, matcher.end(1)) + "PRODUCT_ID>";
			log.info("Input=" + inp + " result=" + res);
		}
		return res;
	} 

	private String matcherSupplierAid(String inp) {
		Pattern pattern = Pattern.compile("<SUPPLIER_AID>(.*)</SUPPLIER_AID>");
		Matcher matcher = pattern.matcher(inp);
		String res = null;
		if(matcher.matches()) {
			res = "<bmecat:SUPPLIER_PID>" +inp.substring(matcher.start(1), matcher.end(1)) + "</bmecat:SUPPLIER_PID>";
			log.info("Input=" + inp + " result=" + res);
		}
		return res;
	} 

	private String matcherOrderUnit(String inp) {
		Pattern pattern = Pattern.compile("<ORDER_UNIT>(.*)</ORDER_UNIT>");
		Matcher matcher = pattern.matcher(inp);
		String res = null;
		if(matcher.matches()) {
			res = "<bmecat:ORDER_UNIT>" +inp.substring(matcher.start(1), matcher.end(1)) + "</bmecat:ORDER_UNIT>";
			log.info("Input=" + inp + " result=" + res);
		}
		return res;
	} 

	private int nextline() throws IOException {
		line = bReader.readLine(); // throws IOException
		log.fine(" line="+line);
		String chg = null;
		if(line==null) {
			pipedSrc.close();
			bReader.close();
			pos = -1;
		} else {
			// Workaround, Umbenennung der Elemente von ARTICLE nach PRODUCT
			chg = matcherArticlId(line.trim());
			if(chg!=null) {
				line = chg;
			}
			// <SUPPLIER_AID>SG8</SUPPLIER_AID> ==> <bmecat:SUPPLIER_PID>SG8</bmecat:SUPPLIER_PID>
			chg = matcherSupplierAid(line.trim());
			if(chg!=null) {
				line = chg;
			}
			// <ORDER_UNIT>PCE</ORDER_UNIT> ==> <bmecat:ORDER_UNIT>PCE</bmecat:ORDER_UNIT>
			chg = matcherOrderUnit(line.trim());
			if(chg!=null) {
				line = chg;
			}

			if("<DISPATCHNOTIFICATION version=\"1.0\">".equals(line.trim())) {
				log.info("CSS inserted and changed to DISPATCHNOTIFICATION version=\"2.0\" ... original-line="+line);
				line = CSS + "\n" + DISPATCHNOTIFICATION20;
			}
			line = line.concat("\n");
			bytes = this.charsetName==null ? line.getBytes() : line.getBytes(charsetName); 
//			// #343 test
//			if(line.contains("ß")) {
//				log.info(line);
//				for(int i=0; i<bytes.length; i++) {
//					//sb.append(String.format("%02X ", b));
//					log.info("i="+i + " byte="+bytes[i] + " ="+String.format("%02X",bytes[i]));
//				}
//			}
			log.fine(" line.length="+line.length() + " bytes.length="+bytes.length);
			pos = 0;
		}
		return pos;
	}
	
	@Override
	public synchronized int read() throws IOException {
		if(line==null)
			return -1;
		if(pos==line.length()) {
			if(nextline()<0) 
				return -1;
		}
//		// #343 test
//		if(bytes[pos]<0) {
//			log.info("pos="+pos + " byte="+bytes[pos] + " ="+String.format("%02X %02X",bytes[pos],bytes[pos+1]));
//		}
		pipedSrc.write(bytes[pos]);
		int in = super.read();
		pos++;
		return in;
	}
	
//	@Override
//	public synchronized int read(byte b[], int off, int len) throws IOException {
//		if(len < line.length()-pos) {
//			pipedSrc.write(bytes, pos, len);
//			pos =+ len;
//			return super.read(b, off, len);
//		}
//		int l = line.length()-pos;
//		log.info(" len="+len + " >=l="+l);
//		pipedSrc.write(bytes, pos, l);
//		int ret = super.read(b, off, l);
//		nextline();
//		return ret;
//	}
	
//	 static final String RESOURCES_DIR = "test/resources/";
//	 static final String SAMPLE_XML = "sample_order_opentrans_2_1_xml signature.xml";
//	private static final String DISPATCHNOTIFICATION_XML = "DESADV_13080388-8662.xml";
//
//	public static void main(String[] args) {
//		InputStream is = null;
//		try {
//		    is = new AvisPipedInputStream(RESOURCES_DIR + DISPATCHNOTIFICATION_XML);
//			for(int i=0; i<10000 ;i++) {
//				is.read();
//			}
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		finally {
//			try {
//				is.close();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//	}

}
