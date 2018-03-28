package com.klst.opentrans;

import static javax.xml.bind.JAXBContext.newInstance;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.logging.Logger;

import javax.inject.Named;
import javax.inject.Singleton;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.opentrans.xmlschema._2.DISPATCHNOTIFICATION;
import org.opentrans.xmlschema._2.OPENTRANS;
import org.opentrans.xmlschema._2.ORDER;
import org.xml.sax.SAXException;

@Named
@Singleton
public class Transformer {

	private static final Logger LOG = Logger.getLogger(Transformer.class.getName());
	private static final String MARSHALLING_ERROR = "Marshalling error";
	private static final String OPENTRANS_XSD_20 = "/opentrans_2_0.xsd";
	private static final String OPENTRANS_XSD_21 = "/opentrans_2_1.xsd";
	private static final String CONTENT_PATH = "org.opentrans.xmlschema._2"; // package in opentrans-jar with ObjectFactory.class
   
	private final JAXBContext jaxbContext;

	public Transformer() {
		LOG.info("ctor");
		try {
			this.jaxbContext = newInstance(CONTENT_PATH);
		} catch (JAXBException e) {
			throw new TransformationException("Could not instantiate JaxB Context", e);
		}
	}

	Transformer(JAXBContext jaxbContext) {
		this.jaxbContext = jaxbContext;
	}

	public Validator getSchemaValidator() throws SAXException {
		SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
		URL otSchemaURL = Transformer.class.getResource(OPENTRANS_XSD_20);
		Schema otSchema = sf.newSchema(otSchemaURL);
		return otSchema.newValidator();
	}

	public OPENTRANS toModel(InputStream xmlInputStream) {
		LOG.info("toModel returns OPENTRANS Object from xmlInputStream="+xmlInputStream);
		try {
			Unmarshaller unmarshaller = createUnmarshaller();
			return unmarshaller.unmarshal(new StreamSource(xmlInputStream), OPENTRANS.class).getValue();
		} catch (JAXBException e) {
			throw new TransformationException(MARSHALLING_ERROR, e);
		}
	}
	
	@SuppressWarnings("unchecked")
	public OPENTRANS toModel(File file) {
		try {
			Unmarshaller unmarshaller = createUnmarshaller();
			return ((JAXBElement<OPENTRANS>) unmarshaller.unmarshal(file)).getValue();
		} catch (JAXBException e) {
			throw new TransformationException(MARSHALLING_ERROR, e);
		}
	}

	public DISPATCHNOTIFICATION toAvis(InputStream xmlInputStream) {
		LOG.info("toModel returns OPENTRANS Object from xmlInputStream="+xmlInputStream);
		try {
			Unmarshaller unmarshaller = createUnmarshaller();
			return unmarshaller.unmarshal(new StreamSource(xmlInputStream), DISPATCHNOTIFICATION.class).getValue();
		} catch (JAXBException e) {
			throw new TransformationException(MARSHALLING_ERROR, e);
		}
	}
	
	@SuppressWarnings("unchecked")
	public DISPATCHNOTIFICATION toAvis(File file) {
		try {
			Unmarshaller unmarshaller = createUnmarshaller();
			return ((JAXBElement<DISPATCHNOTIFICATION>) unmarshaller.unmarshal(file)).getValue();
		} catch (JAXBException e) {
			throw new TransformationException(MARSHALLING_ERROR, e);
		}
	}

/* TODO marshal fromModel // wird nicht benötigt
 
	public byte[] fromModel(OPENTRANS ot) {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream(16000);
		try {
			Marshaller marshaller = createMarshaller();
			marshaller.marshal(ot, outputStream);
		} catch (JAXBException e) {
			throw new TransformationException(MARSHALLING_ERROR, e);
		}
		return outputStream.toByteArray();
	}

	public void fromModel(OPENTRANS ot, OutputStream outputStream) {
		try {
			Marshaller marshaller = createMarshaller();
			marshaller.marshal(ot, outputStream);
		} catch (JAXBException e) {
			throw new TransformationException(MARSHALLING_ERROR, e);
		}
	}

	public void fromModelAsync(final OPENTRANS ot, final OutputStream outputStream) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				fromModel(ot, outputStream);
				try {
					outputStream.flush();
					outputStream.close();
				} catch (IOException e) {
					LOG.log(WARNING, "Faild to Transform Model", e);
				}
			}
		}).start();
	}

 */

	private Unmarshaller createUnmarshaller() throws JAXBException {
		return jaxbContext.createUnmarshaller();
	}

	private Marshaller createMarshaller() throws JAXBException {
		Marshaller marshaller = jaxbContext.createMarshaller();
		marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, formatXmlOutput());
		return marshaller;
	}

	protected Boolean formatXmlOutput() {
		return Boolean.FALSE;
	}

// -----------------
//	private static final String SOE_XML = "SOE-order_FH_31234_8566_2014-09-09-0.724.xml";
//	private static final String DISPATCHNOTIFICATION_XML = "DESADV_13080388-8662.xml";
//
//	public static void main(String[] args) {
//		LOG.info("main");
//		
//		//System.setProperty("jaxp.debug","1"); // liefert:
///*
//JAXP: find factoryId =javax.xml.parsers.SAXParserFactory
//JAXP: loaded from fallback value: com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl
//JAXP: created new instance of class com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl using ClassLoader: null
// */
//		Transformer transformer = new Transformer();
//		String uri = "src/test/resources/"+SOE_XML;
//		
//		try {
//			Source xmlFile = new StreamSource(new File(uri));
//			Validator validator = transformer.getSchemaValidator();
//			validator.validate(xmlFile);
//		} catch (Exception e) {	
//			LOG.severe(e.getMessage());
//		}
//
//		LOG.info("\ncheck transformer.toModel");
//		OPENTRANS ot;
//		try {
//			File file = new File(uri);
//			InputStream is = new FileInputStream(file);
//			ot = transformer.toModel(is);
//			ORDER order = ot.getORDER();
//			LOG.info("order.Version="+order.getVersion());
//			LOG.info("order...ORDERID="+order.getORDERHEADER().getORDERINFO().getORDERID());
//			LOG.info("order.ORDERITEMLIST.size="+order.getORDERITEMLIST().getORDERITEM().size());
//			LOG.info("order...TOTALITEMNUM="+order.getORDERSUMMARY().getTOTALITEMNUM());
//		} catch (Exception e) {
//			LOG.severe(e.getMessage());
//		}
//		
//		try {
//			File file = new File("src/test/resources/"+DISPATCHNOTIFICATION_XML);
//			InputStream is = new FileInputStream(file);
//			DISPATCHNOTIFICATION avis = transformer.toAvis(is);
//			LOG.info("avis.Version="+avis.getVersion());
//		} catch (Exception e) {
//			e.printStackTrace();
//			LOG.severe(e.getMessage());
//		}
//	}
//
}
