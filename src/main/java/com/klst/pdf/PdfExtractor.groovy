// Feature #1467 - ttp: Extrakt von Ausgangsrechnungen ad_archive
package com.klst.pdf

import groovy.lang.Binding
import groovy.lang.Script

class PdfExtractor extends Script {
	
	def CLASSNAME = this.getClass().getName()

	public PdfExtractor() {
		println "${CLASSNAME}:ctor"
	}

	public PdfExtractor(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}
	
	def msg = new StringBuilder()
	def addMsg = { it , m=this.msg ->
		println "${it}"
		m.append("<br/> ${it}")
		return m
	}
	def _isPCLT = null // values P C L T for Process, Callout, ...
	def _Ctx = null // A_Ctx - the context
	def _TrxName = null // A_TrxName
	def _pi = null // Object of MPInstance whith A_AD_PInstance_ID
	def paraList = []
	def paraDict = [:]
	def ctx = { return this.getProperty("A_Ctx") }
	def mapEvent = ["P":"A_Trx","C":"A_WindowNo","L":"A_AD_Org_ID","T":"A_PO"]
	def tryEvent = { it="eventtype"->
		try {
			if(this.getProperty(mapEvent.get(it))) {
				this._isPCLT=it
			} else {
				println "${CLASSNAME}: is not ${it}"
			}
		} catch(MissingPropertyException e) { // expected
//			println "${CLASSNAME}:${it} expected exception "+e.getMessage()
		}
	}
	def eventtype = { it="eventtype"->
		if(this._isPCLT==null) {
			try {
				if(ctx()) {
//					println "${CLASSNAME}:${it} A_Ctx - the context: ${ctx()}"
				} else {
					this._isPCLT="?"
					println "${CLASSNAME}:${it} ***not valid***"
				}
			} catch(MissingPropertyException e) {
				this._isPCLT="e"
				println "${CLASSNAME}:${it} exception "+e.getMessage()
				return this._isPCLT
			}
			if(this._isPCLT) { return this._isPCLT } else {tryEvent("C") }
			if(this._isPCLT) { return this._isPCLT } else {tryEvent("P") }
			if(this._isPCLT) { return this._isPCLT } else {tryEvent("L") }
			if(this._isPCLT) { return this._isPCLT } else {tryEvent("T") }
			if(this._isPCLT) { return this._isPCLT }
		}
	}
	def isCallout = { it="C" ->
		return eventtype()==it
	}
	// dynamisch geladene AD Klassen
	Class MPInstance = null
	Class DB = null //org.compiere.util.DB
	Class MArchive = null
	Class MInvoice = null //org.compiere.model.MInvoice
	def isProcess = { it="P" ->
		def ret = eventtype()==it
		if(ret) {
/*
 PCLT A_Ctx - the context
 P    A_Trx - the transaction
 P    A_TrxName
 P    A_Record_ID
 P L  A_AD_Client_ID
 P L  A_AD_User_ID
 P    A_AD_PInstance_ID --- hiermit komme ich an AD_Process_ID: select * from AD_PInstance ==> class MPInstance und die Parameter
 P    A_Table_ID
  */		 
			this._Ctx = A_Ctx
			this._TrxName = A_TrxName
			println "${CLASSNAME}:isProcess A_AD_PInstance_ID: ${A_AD_PInstance_ID}"
			MPInstance = this.class.classLoader.loadClass("org.compiere.model.MPInstance", true, false )
			println "${CLASSNAME}:isProcess MPInstance: ${MPInstance}"
			this._pi = MPInstance.newInstance(this._Ctx, A_AD_PInstance_ID, "ignored") // letzte para muss String sein
			println "${CLASSNAME}:isProcess this._pi=${this._pi}"
			this.paraList = this._pi.getParameters() // liefert MPInstancePara[] 
			for (para in paraList) {
				println "${CLASSNAME}:isProcess para ${para.getParameterName()}"
				if(para.getParameterName()=="DateFrom") {
					println "${CLASSNAME}:isProcess ${para.getParameterName()} ${para.getP_Date()} - ${para.getP_Date_To()}"
					this.paraDict.put(para.getParameterName(), para.getP_Date()) // getP_Date_To
					this.paraDict.put("DateTo", para.getP_Date_To())
				} else if(para.getParameterName()=="DateTo") {
//					println "${CLASSNAME}:isProcess ${para.getParameterName()} ${para.getP_Date()}"
//					this.paraDict.put(para.getParameterName(), para.getP_Date())
//				} else if(para.getParameterName()=="IsSOTrx") {
//					println "${CLASSNAME}:isProcess ${para.getParameterName()} ${para.getP_String()}"
//					this.paraDict.put(para.getParameterName(), para.getP_String())
				} else {
					this.paraDict.put(para.getParameterName(), para.getP_String())
					println "${CLASSNAME}:isProcess para ${para.getP_String()}"
				}
			}
			DB = this.class.classLoader.loadClass("org.compiere.util.DB", true, false )
			MInvoice = this.class.classLoader.loadClass("org.compiere.model.MInvoice", true, false )
			MArchive = this.class.classLoader.loadClass("org.compiere.model.MArchive", true, false )
		}
		return ret
	}

	def getPara = { it ->
		for (para in paraList) {
			if(para.getParameterName()==it) {
//				println "${CLASSNAME}:getPara ${it} ${para.getP_String()}"
				return para.getP_String()
			}
		}
//		println "${CLASSNAME}:getPara '${it}' nicht gefunden"
	}

	def makeFilename = { documentNo , isCreditMemo , docStatus , ext=".pdf"->
		def prefix = isCreditMemo ? "Gutschrift_" : "Rechnung_"
		def suffix = docStatus=="RE" ? "_storniert" : ""
		def documentName = new StringBuilder()
		documentName.append(prefix)
		documentName.append(documentNo)
		documentName.append(suffix)
		documentName.append(ext)
		return documentName.toString()
	}
/*
select ad_archive_id,binarydata from ad_archive where ad_table_id in(select ad_table_id from ad_table where tablename='C_Invoice') -- 318
and record_id in(select C_Invoice_id from C_Invoice where isactive='Y' and issotrx='Y' and dateinvoiced>'2018-01-01')
and ad_archive_id=${ad_archive_id}

com.klst.pdf.PdfExtractor:getArchive
SELECT * FROM ad_archive
WHERE ad_client_id = 1000000 AND ad_org_id IN( 0 , 1000000 ) AND isactive = 'Y'
  AND ad_table_id in(select ad_table_id from ad_table where tablename='C_Invoice')
  AND record_id = ?
 1:1044146
com.klst.pdf.PdfExtractor:getArchive MArchive[1002501,Name=23617]
com.klst.pdf.PdfExtractor:getArchive binaryData.length=68931
com.klst.pdf.PdfExtractor:getArchive MArchive[1002502,Name=23617]
com.klst.pdf.PdfExtractor:getArchive binaryData.length=68931
com.klst.pdf.PdfExtractor:getArchive MArchive[1002519,Name=23617]
com.klst.pdf.PdfExtractor:getArchive binaryData.length=68931
c
 */
	// returns 
	def getArchive = { File destFile, ad_client_id, ad_org_id, invoice, tableName='C_Invoice', trxName=this._TrxName, ctx=this._Ctx ->
		println "${CLASSNAME}:getArchive invoice.status=${invoice.getDocStatus()} isCreditMemo=${invoice.isCreditMemo()} ${invoice}"
		def record_id = invoice.get_ID()
		def sql = """
SELECT * FROM ad_archive
WHERE ad_client_id = ${ad_client_id} AND ad_org_id IN( 0 , ${ad_org_id} ) AND isactive = 'Y'
  AND ad_table_id in(select ad_table_id from ad_table where tablename='${tableName}')
  AND record_id = ?
"""
		def pstmt = null
		println "${CLASSNAME}:getArchive ${sql} 1:${record_id}"
		pstmt = DB.prepareStatement(sql, trxName)
		pstmt.setInt(1, record_id)
		def resultSet = pstmt.executeQuery()
		def obj = null
		def numArch = 0
		if(resultSet) {
			while(resultSet.next()) {
				numArch++
				obj = MArchive.newInstance(ctx, resultSet, trxName)
				println "${CLASSNAME}:getArchive ${obj}"
//				byte[] binaryData = obj.getBinaryData() 
//				println "${CLASSNAME}:getArchive binaryData.length=${binaryData.length}"
			}
		}
		if(numArch>1) {
			println "${CLASSNAME}:getArchive ${numArch} Archive gefunden für ${invoice}"
			addMsg("${numArch} Archive gefunden für ${invoice}")
		}
		if(obj==null) {
			println "${CLASSNAME}:getArchive kein Archive gefunden für ${invoice}"
			addMsg("kein Archiv gefunden für ${invoice}")
			return
		} else {
			println "${CLASSNAME}:getArchive ad_archive.Name:${obj.getName()} invoice.DocumentNo:${invoice.getDocumentNo()} status=${invoice.getDocStatus()} isCreditMemo=${invoice.isCreditMemo()}"
//			println "${CLASSNAME}:getArchive ${makeFilename(invoice.getDocumentNo(),invoice.isCreditMemo(),invoice.getDocStatus())}"
		}
		// inflate to pdf
		byte[] binaryData = obj.getBinaryData()
//		def filename = makeFilename(invoice.getDocumentNo(), invoice.isCreditMemo(), invoice.getDocStatus())
//		def m_archivePathRoot = File.separator +"tmp" // TODO param
//		final File destFile = new File(m_archivePathRoot + File.separator + filename+".pdf")
		println "${CLASSNAME}:getArchive write to to ${destFile} binaryData.length=${binaryData.length}"
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destFile))
		bos.write(binaryData)
		bos.flush()
		bos.close()
		return binaryData.length
				
//		byte[] deflatedData = obj.getBinaryData()
//		println "${CLASSNAME}:getArchive inflate to ${filename} - deflatedData.length=${deflatedData.length}"
//		byte[] inflatedData = null;
//		ByteArrayInputStream bais = new ByteArrayInputStream(deflatedData)
//		ZipInputStream zip = new ZipInputStream(bais)
//		ZipEntry entry = zip.getNextEntry()
////		if(entry != null) {
//			ByteArrayOutputStream out = new ByteArrayOutputStream();
//			byte[] buffer = new byte[2048];
//			int length = zip.read(buffer);
//			while (length != -1) {
//				out.write(buffer, 0, length);
//				length = zip.read(buffer);
//			}
//			inflatedData = out.toByteArray();
//			println "${CLASSNAME}:getArchive ${filename} : inflatedData.length=${inflatedData.length}"
//// - zip=${entry.getCompressedSize()}(${entry.getSize()})."
//				
//				//private void saveBinaryDataIntoFileSystem(byte[] inflatedData) {
//			def m_archivePathRoot = File.separator +"tmp"
//			final File destFile = new File(m_archivePathRoot + File.separator + filename+".pdf");
//			BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destFile));
//			bos.write(inflatedData);
//			bos.flush();
////		}
	}
	
	/*
groovy Klasse: com.klst.pdf.PdfExtractor soll für Archive und Attachments/Feature #1510 möglichst gleich sein
ad_prozess im Menu Verkauf/Verkaufsrechnungen: @script:groovy:ArchivesToPdf mit folgenden Parametern
* dateFrom, dateTo : Zeitraum für c_invoice.dateinvoiced
* (default) c_invoice.issotrx='Y'
Zielverzeichnis für die extrahierten Belege
Belegstatus c_invoice.docstatus in ('CO','CL','RE') -- keine unfertigen, wie in issue #1436#note-4
Prefixstring (optional), der Prefix kann aus den Dokumenttypen (Rechnung/Gutschtift) per default ermittelt werden

select * from AD_Ref_List where AD_Reference_id in( select AD_Reference_id from AD_Reference where AD_Reference_ID=131) and value IN('CO','CL','RE')

	 */
	def getInvoices = { ad_client_id, ad_org_id, dateFrom, dateTo, issotrx='N', docStatus3='YX', tableName='C_Invoice', trxName=this._TrxName, ctx=this._Ctx ->
		def sql = """
SELECT * FROM ${tableName}
WHERE ad_client_id = ${ad_client_id} AND ad_org_id IN( 0 , ${ad_org_id} ) AND isactive = 'Y'
  AND issotrx = '${issotrx}'
  AND dateinvoiced between ? and ?
  AND docstatus IN('CO','CL','${docStatus3}')
"""
		def pstmt = null
		println "${CLASSNAME}:getInvoices ${sql} 1:${dateFrom} 2:${dateTo}"
		pstmt = DB.prepareStatement(sql, trxName)
		def nDateFrom = 1
		def nDateTo = 2
		if(dateFrom>dateTo) { // dateFrom + dateTo vertauschen
			addMsg("WARN DateRange FROM ${dateFrom} TO ${dateTo} : From>To (wird zur Korrektur getauscht)")
			nDateFrom = 2
			nDateTo = 1
		} else {
			addMsg("INFO DateRange FROM ${dateFrom} TO ${dateTo}")
		}
		pstmt.setTimestamp(nDateFrom, dateFrom)
		pstmt.setTimestamp(nDateTo, dateTo)
		def resultSet = pstmt.executeQuery()
		def obj = null
		def result = [] // List of invoices
		if(resultSet) {
			while(resultSet.next()) {
				obj = MInvoice.newInstance(ctx, resultSet, trxName)
				println "${CLASSNAME}:getInvoices status=${obj.getDocStatus()} isCreditMemo=${obj.isCreditMemo()} ${obj}"
				result.add(obj)
			}
		}
		if(obj==null) {
			println "${CLASSNAME}:getInvoices keine Rechnung gefunden"
			addMsg("keine Rechnung gefunden für Zeitraum ${dateFrom} bis ${dateTo}")
		}
		println "${CLASSNAME}:getInvoices ${result.size()} Rechnung gefunden"
		return result
	}

	/**
	 * AD_Process
	 * @param DateFrom + DateTo
	 * @param File_Directory
	 * @param Prefix
	 * @param IsSOTrx
	 * @param DocStatus3
	 */
	@Override
	public Object run() {
		println "${CLASSNAME}:run"
		if(isProcess()) try {
			File dir = new File(this.paraDict.get("File_Directory"))
			if(!dir.isDirectory()) {
				return dir.getAbsolutePath()+" is not a Directory" 
			}
			def invoices = getInvoices(this.getProperty("A_AD_Client_ID"), 1000000
				, this.paraDict.get("DateFrom"), this.paraDict.get("DateTo")
				, this.paraDict.get("IsSOTrx"), this.paraDict.get("DocStatus3")) 
			
			def files = 0
			println "${CLASSNAME}:run ${invoices.size()}"
			invoices.each{ inv ->
				def filename = makeFilename(inv.getDocumentNo(), inv.isCreditMemo(), inv.getDocStatus())
				File file = new File(dir, filename)
				def bytesWritten = getArchive(file, this.getProperty("A_AD_Client_ID"), 1000000, inv)
				if(bytesWritten>0) {
					files++
				}
			}
			addMsg("invoices/files : ${invoices.size()}/${files}")
/*
com.klst.pdf.PdfExtractor:ctor
com.klst.pdf.PdfExtractor:run
com.klst.pdf.PdfExtractor:isProcess A_AD_PInstance_ID: 2475272
com.klst.pdf.PdfExtractor:isProcess MPInstance: class org.compiere.model.MPInstance
com.klst.pdf.PdfExtractor:isProcess this._pi=MPInstance[2475272,OK=false]
com.klst.pdf.PdfExtractor:isProcess para IsSOTrx
com.klst.pdf.PdfExtractor:isProcess para Y
com.klst.pdf.PdfExtractor:isProcess para DateFrom
com.klst.pdf.PdfExtractor:isProcess DateFrom 2018-01-01 00:00:00.0 - 2018-02-01 00:00:00.0
com.klst.pdf.PdfExtractor:isProcess para File_Directory
com.klst.pdf.PdfExtractor:isProcess para x
com.klst.pdf.PdfExtractor:isProcess para extraPrefix
com.klst.pdf.PdfExtractor:isProcess para N
com.klst.pdf.PdfExtractor:isProcess para Prefix
com.klst.pdf.PdfExtractor:isProcess para Rechnung_
com.klst.pdf.PdfExtractor:isProcess para DocStatus3
com.klst.pdf.PdfExtractor:isProcess para RE
com.klst.pdf.PdfExtractor:getInvoices
 */
		} catch(Exception e) {
			return e.getMessage()
		} else {
			println "${CLASSNAME}:run noProcess"
		}
		println "${this.msg}"
		return this.msg
	}
	
	// wird in eclipse benötigt, damit ein "Run As Groovy Script" möglich ist (ohne Inhalt)
	// nach dem Instanzieren mit Binding wird run() ausgeführt
	// Will man den ctor ohne Binding, dann den kommentierten Code nutzen
	static main(args) {		
//		Script script = new OPOStoPayments()
//		script.run()
	}
  
}
