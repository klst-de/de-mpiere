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
	
	// returns num of bytes written
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
			}
		}
		if(numArch>1) {
			println "${CLASSNAME}:getArchive ${numArch} archives for ${invoice} : got ${obj}"
			addMsg("${numArch} archives for ${invoice} : got ${obj}")
		}
		if(obj==null) {
			println "${CLASSNAME}:getArchive no archive for ${invoice}"
			addMsg("no archive for ${invoice}")
			return
		} else {
			println "${CLASSNAME}:getArchive ad_archive.Name:${obj.getName()} invoice.DocumentNo:${invoice.getDocumentNo()} status=${invoice.getDocStatus()} isCreditMemo=${invoice.isCreditMemo()}"
		}
		
		byte[] binaryData = obj.getBinaryData()
		println "${CLASSNAME}:getArchive write to to ${destFile} binaryData.length=${binaryData.length}"
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destFile))
		bos.write(binaryData)
		bos.flush()
		bos.close()
		return binaryData.length
				
	}
	
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
			addMsg("WARN DateRange FROM ${dateFrom} TO ${dateTo} : From>To (adjusted)")
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
			println "${CLASSNAME}:getInvoices no invioces"
			addMsg("no invioces for dateinvoiced BETWEEN ${dateFrom} AND ${dateTo}")
		}
		println "${CLASSNAME}:getInvoices ${result.size()} invioces found"
		return result
	}

	/**
	 * AD_Process
	 * @param IsSOTrx : Y for getArchive
	 * @param DateFrom + DateTo
	 * @param File_Directory
	 * @param extraPrefix + Prefix - inactiv
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
