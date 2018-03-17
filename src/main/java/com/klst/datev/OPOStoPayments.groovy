// Feat #1417 - Bezahlte Rechnungen datev->AD
package com.klst.datev

import groovy.lang.Binding
import groovy.lang.Script

import java.text.DecimalFormat
import java.text.ParsePosition
import java.text.SimpleDateFormat
import java.util.Properties

import org.codehaus.groovy.GroovyException
import org.codehaus.groovy.scriptom.ActiveXObject

import com.jacob.com.ComFailException

class OPOStoPayments extends Script {
	
	def CLASSNAME = this.getClass().getName()

	public OPOStoPayments() {
		println "${CLASSNAME}:ctor"
	}

	public OPOStoPayments(Binding binding) {
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
	Class MBPartner = null
	Class MInvoice = null //org.compiere.model.MInvoice
	Class MPayment = null 
	Class MAllocationHdr = null 
	Class MAllocationLine = null
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
				//def v = para.get_Value()
				println "${CLASSNAME}:isProcess para ${para.getParameterName()}"
				//this.paraDict.put(para.getParameterName(), v)
//				if(para.getParameterName()=="FileName") {
//					this.pXls = para.getP_String()
//					println "${CLASSNAME}:isProcess ${para.getParameterName()} ${para.getP_String()}"
//				}
			}
			DB = this.class.classLoader.loadClass("org.compiere.util.DB", true, false )
			MBPartner = this.class.classLoader.loadClass("org.compiere.model.MBPartner", true, false )
			MInvoice = this.class.classLoader.loadClass("org.compiere.model.MInvoice", true, false )
			MPayment = this.class.classLoader.loadClass("org.compiere.model.MPayment", true, false )
			MAllocationHdr = this.class.classLoader.loadClass("org.compiere.model.MAllocationHdr", true, false )
			MAllocationLine = this.class.classLoader.loadClass("org.compiere.model.MAllocationLine", true, false )
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

	private static final ACTIVEX_FSO = "Scripting.FileSystemObject"
	private static final ACTIVEX_EXCEL = "Excel.Application"
	def getExcel = { ->
		def objExcel = new ActiveXObject(ACTIVEX_EXCEL)
		println "${CLASSNAME}:getExcel Version ${objExcel.Version}"
		return objExcel
	}
	
	private static final CSV_EXT = ".csv"
	private static final XLSDIR_TEST = "C:\\proj\\minhoff\\DATEV\\"
	def getSheet = { excel, file=getPara("FileName") ->
		println "${CLASSNAME}:getSheet ${file}"
		def workbook = excel.Workbooks.Open(file, 0 , true) // open ro
		def sheets = workbook.Worksheets
		println "${CLASSNAME}:getSheet sheets.Count=${sheets.Count}"
		try {
			def sheet = workbook.Worksheets(1)
			println "${CLASSNAME}:getSheet sheet=${sheet}"
			return sheet
		} catch(ComFailException e) {
			println "${CLASSNAME}:getSheet cannot find sheet"
			throw e
		}
	}

	def getBPartner = { ad_client_id, ad_org_id, value, trxName=this._TrxName, ctx=this._Ctx ->
		def sql = """
SELECT * FROM c_bpartner
WHERE ad_client_id = ${ad_client_id} AND ad_org_id IN( 0 , ${ad_org_id} ) AND isactive = 'Y'
  AND value = '${value}'
"""
//		println "${CLASSNAME}:getBPartner ${sql}"
		def pstmt = DB.prepareStatement(sql, trxName)
		def resultSet = pstmt.executeQuery()
		def obj = null
		if(resultSet) {
			while(resultSet.next()) {
				obj = MBPartner.newInstance(ctx, resultSet, trxName)
				println "${CLASSNAME}:getMBPartner ${obj}"
			}
		}
		if(obj==null) {
			println "${CLASSNAME}:getBPartner keine BP gefunden für '${value}'"
		}
		return obj
	}

	def getInvoice = { ad_client_id, ad_org_id, docno, issotrx='N', c_bpartner_id=0,dateinvoiced=0,grandtotal=0, trxName=this._TrxName, ctx=this._Ctx ->
		def sql = """
SELECT * FROM c_invoice
WHERE ad_client_id = ${ad_client_id} AND ad_org_id IN( 0 , ${ad_org_id} ) AND isactive = 'Y'
  AND documentno = '${docno}'
  AND issotrx = '${issotrx}'
"""
		// wg. https://projects.klst.com/issues/1472
		def sql2 = """
SELECT * FROM c_invoice
WHERE ad_client_id = ${ad_client_id} AND ad_org_id IN( 0 , ${ad_org_id} ) AND isactive = 'Y'
  AND documentno = '10${docno}'
  AND c_bpartner_id = ${c_bpartner_id}
  AND issotrx = '${issotrx}'
"""
		def sql3 = """
SELECT * FROM c_invoice
WHERE ad_client_id = ${ad_client_id} AND ad_org_id IN( 0 , ${ad_org_id} ) AND isactive = 'Y'
  AND c_bpartner_id = ?
  AND dateinvoiced = ?
  AND grandtotal = ?
  AND issotrx = '${issotrx}'
"""
		def pstmt = null
		if(docno==null) {
			println "${CLASSNAME}:getInvoice ${sql3} 1:${c_bpartner_id} 2:${dateinvoiced} 3:${grandtotal}"
			pstmt = DB.prepareStatement(sql3, trxName)
			pstmt.setInt(1, c_bpartner_id)
			pstmt.setTimestamp(2, dateinvoiced)
			pstmt.setBigDecimal(3, grandtotal)
		} else if(c_bpartner_id>0) {
			println "${CLASSNAME}:getInvoice ${sql2} 1:${c_bpartner_id} 2:${dateinvoiced} 3:${grandtotal}"
			pstmt = DB.prepareStatement(sql2, trxName)
		} else {
//			println "${CLASSNAME}:getInvoice ${sql}"
			pstmt = DB.prepareStatement(sql, trxName)
		}
		def resultSet = pstmt.executeQuery()
		def obj = null
		if(resultSet) {
			while(resultSet.next()) {
				obj = MInvoice.newInstance(ctx, resultSet, trxName)
				println "${CLASSNAME}:getInvoice ${obj}"
			}
		}
		if(obj==null) {
			println "${CLASSNAME}:getInvoice keine Rechnung gefunden für '${docno}'"
		}
		return obj
	}

	// isreceipt == Zahlungseingang
	def getPayment = { ad_client_id, ad_org_id, docno, isreceipt='N', trxName=this._TrxName, ctx=this._Ctx ->
		def sql = """
SELECT * FROM c_payment
WHERE ad_client_id = ${ad_client_id} AND ad_org_id IN( 0 , ${ad_org_id} ) AND isactive = 'Y'
  AND documentno = '${docno}'
  AND isreceipt = '${isreceipt}'
"""
		println "${CLASSNAME}:getPayment ${sql}"
		def pstmt = DB.prepareStatement(sql, trxName)
		def resultSet = pstmt.executeQuery()
		def obj = null
		if(resultSet) {
			while(resultSet.next()) {
				obj = MPayment.newInstance(ctx, resultSet, trxName)
				println "${CLASSNAME}:getPayment ${obj}"
			}
		}
		if(obj==null) {
			println "${CLASSNAME}:getPayment keine Zahlung gefunden für '${docno}'"
		}
		return obj
	}

	// aus map opos-payment it, siehe opos.makePayment(...) ein AD-Payment Ausgangszahlungsobjekt erstellen
	// isReceipt=false ==> Ausgangszahlung
	def makeMPayment = { it , C_BPartner_ID , isReceipt=false ->
/* nicht immer is kto==BANK
[BP:44157 ...
, payments:[["rec":"170741", "amt":-1192.38, "dat":2017-11-22 00:00:00.0, "kto":"div."]]]

[BP:47373, ...
, payments:[["rec":"20336", "amt":232.05, "dat":2017-12-31 00:00:00.0, "kto":"WriteOff"]]]
c
 */
		if(it.kto==Opos.WRITEOFF) {
			throw new Exception("payment ist Abschreibung, keine Zahlung - ${it}")
		}
		assert it.kto==Opos.BANK || it.kto=="div."
		def beleg = getPayment(this.getProperty("A_AD_Client_ID"), 1000000, it.rec)
		if(beleg) {
			// eine Zahlung sollte nicht existieren, sie wird erst jetzt angelegt
/* ist das tatsächlich so? in DATEV nicht, Beispiel:
BP:83007 ...
payments=[["rec":"20526", "amt":-1761.8, "dat":2017-11-03 00:00:00.0, "kto":"Bank"]
        , ["rec":"20526", "amt":-1761.8, "dat":2017-11-07 00:00:00.0, "kto":"Bank"]
        , ["rec":"20526", "amt":1761.8, "dat":2017-11-15 00:00:00.0, "kto":"Bank"]]
 */
			if(beleg.getC_BPartner_ID()==C_BPartner_ID && beleg.getDateAcct()==it.dat && beleg.getPayAmt()==it.amt) {
				throw new Exception("Zahlung ${it} existiert bereits: ${beleg}")
			} else {
				addMsg("WARN andere Zahlung mit gleicher Belegnr ${beleg} existiert(!)")
				beleg = null
			}
		} 
		if(beleg==null) {
			beleg = MPayment.newInstance(this._Ctx, 0, this._TrxName)
			beleg.setDocumentNo(it.rec)
			beleg.setC_DocType_ID(isReceipt)
			beleg.setC_BankAccount_ID(1000000) // C_BankAccount_ID
			beleg.setTenderType(MPayment.TENDERTYPE_DirectDeposit)
			beleg.setC_BPartner_ID(C_BPartner_ID)
			beleg.setAmount(0,it.amt) // 0 == default Currency , sonst C_Currency_ID
			beleg.setDiscountAmt(it.dis)
			if(isReceipt) {
				// OK
			} else { // Beträge bei Ausgangszahlungen in DATEV negativ! daher: -amt 
				beleg.setAmount(0,beleg.getPayAmt().negate())
				beleg.setDiscountAmt(beleg.getDiscountAmt().negate())
			}
			beleg.setDateAcct(it.dat)
			beleg.saveEx()
			println "${CLASSNAME}:makeMPayment Zahlung angelegt ${beleg}"
			it.C_Payment_ID = beleg.getC_Payment_ID()
		}
		return beleg
	}
	
	def makeAllocFromInvoices = { it , C_Currency_ID ->
		try {
			def alloc = MAllocationHdr.newInstance(this._Ctx, 0, this._TrxName)
			alloc.setC_Currency_ID(C_Currency_ID)
			alloc.saveEx()
			println "${CLASSNAME}:makeAllocFromInvoices alloc angelegt ${alloc}"
			it.invoiceList.each{ invoice ->
				def aLine = MAllocationLine.newInstance(alloc) // Parent ctor
				aLine.setDocInfo(it.C_BPartner_ID,0,invoice.get('C_Invoice_ID'))
				aLine.setAmount(invoice.amt)
				aLine.saveEx()
			}
			def done = alloc.processIt("CO") // String ACTION_Complete = "CO"
			if(done) {			
				addMsg("BP ${it.kto} Zuordnung Rechnungen und Gutschriften status ${alloc.getDocStatus()} - ${alloc}")
			} else {
				throw new Exception("konnte Zuordnung Rechnungen und Gutschriften nicht fertigstellen - ${alloc}")
			}
		} catch(Exception e) {
			println "${CLASSNAME}:makeAllocFromInvoices exception ${e}"
			e.printStackTrace()
			throw e
		}
	}

	// C_DocType_ID:1000005 : API == "Eingangsrechnung"
	// C_DocType_ID:1000009 : APP == "Zahlungsausgang"
	// accounts payable == Kreditorenbuchhaltung Lieferantenverbindlichkeiten
	def makeAPPaymentAllocations = { Allocations it , createInvoice ->
		println "${CLASSNAME}:makeAPPaymentAllocations Eingangsrechnungen und Zahlungsausgang an Lieferanten"
		def C_Currency_ID = 102 // EUR
		def bp = getBPartner(this.getProperty("A_AD_Client_ID"), 1000000, it.kto)
		if(bp==null) {
			throw new Exception("aktiven Lieferant ${it.kto} nicht gefunden")
		}
		it.C_BPartner_ID = bp.getC_BPartner_ID()
		
		it.invoiceList.each{ oi ->		
			// zuerst suchen mit BelegNr oi.rec
			def beleg = getInvoice(this.getProperty("A_AD_Client_ID"), 1000000, oi.rec)
			if(beleg==null) { // dann suchen mit BP ==> 10||oi.rec , wg. Bug https://projects.klst.com/issues/1472
				beleg = getInvoice(this.getProperty("A_AD_Client_ID"), 1000000, oi.rec,'N',bp.getC_BPartner_ID())
			}
			if(beleg==null) { // dann suchen mit Betrag
				beleg = getInvoice(this.getProperty("A_AD_Client_ID"), 1000000, null,'N',bp.getC_BPartner_ID(),oi.dat,new BigDecimal(0).subtract(oi.amt))
				if(beleg) addMsg("WARN Eingangsrechnung ${oi.rec} hat andere Belegnr: ${beleg}")
			}
			
			if(beleg) { // Re existiert
				def isPaid = beleg.isPaid()
				println "${CLASSNAME}:makeAPPaymentAllocations Rechnung gefunden ${beleg} isPaid=${isPaid}"
				if(isPaid) {
					throw new Exception("Rechnung ${oi.rec} bereits bezahlt - ${beleg}")
				}
			} else if(createInvoice) {
				println "${CLASSNAME}:makeAPPaymentAllocations createInvoice C_Currency_ID=${C_Currency_ID}"
				beleg = MInvoice.newInstance(this._Ctx, 0, this._TrxName)
				beleg.setC_DocTypeTarget_ID("API")
				beleg.setBPartner(bp) 
				beleg.setDocumentNo(oi.rec)
				beleg.setIsSOTrx(false)
				beleg.setDateAcct(oi.dat)
				beleg.setGrandTotal(0-oi.amt)
				beleg.setDescription(oi.txt) // description + poreference character varying(20)
/*
com.klst.datev.OPOStoPayments:makeAPPaymentAllocations createInvoice C_Currency_ID=102
19:35:19.767 DB.getSQLValueEx: No Value SELECT M_PriceList_ID FROM M_PriceList WHERE AD_Client_ID=? AND IsDefault='Y' [28]
-----------> MInvoice.org$compiere$model$PO$save$aop: beforeSave - MInvoice[0-1999,GrandTotal=-1183.0] [28]
java.lang.IllegalArgumentException: C_Currency_ID is mandatory.
        at org.compiere.model.PO.set_Value(PO.java:760)
        at org.compiere.model.PO.set_Value(PO.java:708)
        at org.compiere.model.X_C_Invoice.setC_Currency_ID(X_C_Invoice.java:374)
        at org.compiere.model.MInvoice.setM_PriceList_ID(MInvoice.java:1120)
        at org.compiere.model.MInvoice.beforeSave(MInvoice.java:997)
        at org.compiere.model.PO.org$compiere$model$PO$save$aop(PO.java:2052)
        at org.compiere.model.PO$JoinPoint_save_N_3133207336727957698.dispatch(PO$JoinPoint_save_N_3133207336727957698.java)
        at org.compiere.model.JoinPoint_save_N_3133207336727957698_5.invokeNext(JoinPoint_save_N_3133207336727957698_5.java)
        at de.metas.commission.aop.POSaveDelete.invoke(POSaveDelete.java:134)
        at de.metas.commission.aop.POSaveDelete.save(POSaveDelete.java:36)
        at org.compiere.model.JoinPoint_save_N_3133207336727957698_5.invokeNext(JoinPoint_save_N_3133207336727957698_5.java)
        at org.compiere.model.JoinPoint_save_N_3133207336727957698_5.invokeJoinpoint(JoinPoint_save_N_3133207336727957698_5.java)
        at org.compiere.model.PO$POAdvisor.save_N_3133207336727957698(PO$POAdvisor.java)
        at org.compiere.model.PO.save(PO.java)
        at org.compiere.model.PO.saveEx(PO.java:2197)

setM_PriceList_ID ????
Rechnungen ohne Positionen kann man nicht fertigstellen
 */
				//beleg.setC_Currency_ID(C_Currency_ID)
				beleg.setM_PriceList_ID(1000001)
				beleg.saveEx()
				def done = beleg.processIt("CO")
				if(done) {
					//addMsg("Eingangsrechnung angelegt - ${beleg}")
					println "${CLASSNAME}:makeAPPaymentAllocations Eingangsrechnung angelegt - ${beleg}"
				} else {
					addMsg("WARN Eingangsrechnung angelegt, aber nicht fertiggestellt - ${beleg}")
				}
			} else {			
				throw new Exception("Eingangsrechnung ${oi.rec} nicht gefunden")
			}
			oi.C_Invoice_ID = beleg.getC_Invoice_ID()
			C_Currency_ID = beleg.getC_Currency_ID()
		}
		
		if(it.paymentList.size()==0) {
			// kein payment Objekt: Rechnungen und Gutschriften heben sich auf
			makeAllocFromInvoices(it,C_Currency_ID)

		} else if(it.paymentList.size()==1) {
			try {
				def op = it.paymentList[0]
				def beleg = makeMPayment(op,it.C_BPartner_ID)
				if(it.invoiceList.size()==1) {
					beleg.setC_Invoice_ID( it.invoiceList[0].get('C_Invoice_ID') )
					beleg.saveEx()
					def done = beleg.processIt("CO")
					if(done) {
						println "${CLASSNAME}:makeAPPaymentAllocations BP ${it.kto} 1:1 Zahlung angelegt - ${beleg}"
					} else {
						addMsg("WARN BP ${it.kto} Zahlung angelegt, aber nicht fertiggestellt - ${beleg}")
					}
					done = beleg.save()
					println "${CLASSNAME}:makeAPPaymentAllocations save() : ${done}"
				} else {
					def alloc = MAllocationHdr.newInstance(this._Ctx, 0, this._TrxName)
					println "${CLASSNAME}:makeAPPaymentAllocations alloc für 1 payment invoiceList=${it.invoiceList} C_Currency_ID=${C_Currency_ID}"
					alloc.setC_Currency_ID(C_Currency_ID)
					alloc.saveEx()
					println "${CLASSNAME}:makeAPPaymentAllocations alloc angelegt ${alloc}"
					BigDecimal oua = new BigDecimal(0).add(op.amt).add(op.dis==null ? 0 : op.dis) // OverUnderAmt
					it.invoiceList.each{ oi ->
						def aLine = MAllocationLine.newInstance(alloc) // Parent ctor
						aLine.setC_Payment_ID(op.get('C_Payment_ID'))
						aLine.setDocInfo(it.C_BPartner_ID,0,oi.get('C_Invoice_ID')) // 0 == no C_Order_ID
						aLine.setAmount(oi.amt)
						aLine.setDiscountAmt(oi.dis)
						oua = oua.subtract(aLine.getAmount()).subtract(aLine.getDiscountAmt())
						aLine.setOverUnderAmt(oua)
						aLine.saveEx()
						println "${CLASSNAME}:makeAPPaymentAllocations saved - ${aLine}"
					}
					// assert oua==0
					def done = alloc.processIt("CO")
					if(done) {
						println "${CLASSNAME}:makeAPPaymentAllocations BP ${it.kto} n:1 Zahlung angelegt - ${alloc}"
					} else {
						addMsg("WARN BP ${it.kto} Zahlung angelegt, Allocation nicht fertiggestellt - ${alloc}")
					}
					done = alloc.save()
					println "${CLASSNAME}:makeAPPaymentAllocations alloc saved=${done} - ${alloc}"	
				}
			} catch(Exception e) {
				println "${CLASSNAME}:makeAPPaymentAllocations exception ${e}"
				e.printStackTrace()
				throw e
			}

		} else { // n payments
/*
com.klst.datev.OPOStoPayments:getOpos balanced? [BP:83007
, invoices=:[["rec":"20526", "amt":-1761.8, "kto":3400.0, "dat":2017-09-05 00:00:00.0, "txt":"8129485181/40887274 Tech Data ", "ust":19.0]]
, payments ...
com.klst.datev.OPOStoPayments:makeAPPaymentAllocations alloc f³r paymentList=
[["rec":"20526", "amt":-1761.8, "dat":2017-11-03 00:00:00.0, "kto":"BANK", "C_Payment_ID":1024436]
, ["rec":"20526", "amt":-1761.8, "dat":2017-11-07 00:00:00.0, "kto":"BANK", "C_Payment_ID":1024437]
, ["rec":"20526", "amt":1761.8, "dat":2017-11-15 00:00:00.0, "kto":"BANK", "C_Payment_ID":1024438]] C_Currency_ID=102
 */
			try {
				it.paymentList.each{ op -> 
					def beleg = makeMPayment(op,it.C_BPartner_ID)
					def done = beleg.processIt("CO")
					println "${CLASSNAME}:makeAPPaymentAllocations beleg completed=${done} - ${beleg}"
					done = beleg.save()
					println "${CLASSNAME}:makeAPPaymentAllocations beleg saved=${done} - ${beleg}"
				}
				def alloc = MAllocationHdr.newInstance(this._Ctx, 0, this._TrxName)
				println "${CLASSNAME}:makeAPPaymentAllocations alloc für paymentList=${it.paymentList} C_Currency_ID=${C_Currency_ID}"
				alloc.setC_Currency_ID(C_Currency_ID)
				alloc.saveEx()
				println "${CLASSNAME}:makeAPPaymentAllocations alloc angelegt ${alloc}"
				
				// zuerst aLine für payments:
				BigDecimal oua = new BigDecimal(0) // OverUnderAmt
				it.paymentList.each{ op ->
					def aLine = MAllocationLine.newInstance(alloc) // Parent ctor
					aLine.setC_Payment_ID(op.get('C_Payment_ID'))
					aLine.setDocInfo(it.C_BPartner_ID,0,-1) // -1 keine Rechnung
					aLine.setAmount(op.amt)
					aLine.setDiscountAmt(op.dis)
					oua = oua.add(aLine.getAmount()).add(aLine.getDiscountAmt())
					aLine.setOverUnderAmt(oua)
					aLine.saveEx()
					println "${CLASSNAME}:makeAPPaymentAllocations saved - ${aLine}"
				}
				// jetzt die rechnungen
				it.invoiceList.each{ oi ->
					def aLine = MAllocationLine.newInstance(alloc) // Parent ctor
					aLine.setDocInfo(it.C_BPartner_ID,0,oi.get('C_Invoice_ID')) 
					aLine.setAmount(oi.amt)
					oua = oua.subtract(aLine.getAmount()).subtract(aLine.getDiscountAmt())
					aLine.setOverUnderAmt(oua)
					aLine.saveEx()
					println "${CLASSNAME}:makeAPPaymentAllocations saved - ${aLine}"
				}
				
				def done = alloc.processIt("CO")
				println "${CLASSNAME}:makeAPPaymentAllocations alloc completed=${done} - ${alloc}"
				done = alloc.save()
				println "${CLASSNAME}:makeAPPaymentAllocations alloc saved=${done} - ${alloc}"
				
			} catch(Exception e) {
				println "${CLASSNAME}:makeAPPaymentAllocations exception ${e}"
				e.printStackTrace()
				throw e
			}
		} // fi
		return it.paymentList.size()
	}
	
	// C_DocType_ID:1000002 : ARI == (Ausgangs)"Rechnung"
	// C_DocType_ID:1000008 : ARR == "Zahlungseingang"
	// accounts receivable == Außenstände Debitoren Debitorenbuchhaltung Forderungen offene Forderungen
	def makeARReceiptAllocations = { Allocations it , createInvoice ->
		println "${CLASSNAME}:makeARReceiptAllocations Ausgangsrechnungen und Eingangazahlungen von Kunden"
		def C_Currency_ID = 102 // EUR
		def bp = getBPartner(this.getProperty("A_AD_Client_ID"), 1000000, it.kto)
		if(bp==null) {
			throw new Exception("aktiven Kunden ${it.kto} nicht gefunden")
		}
		it.C_BPartner_ID = bp.getC_BPartner_ID()
		
		it.invoiceList.each{ invoice ->
			def beleg = getInvoice(this.getProperty("A_AD_Client_ID"), 1000000, invoice.rec, 'Y')
			// check beleg == invoice : bp betrag datum 
			if(beleg) {
				C_Currency_ID = beleg.getC_Currency_ID()
				if(beleg.getC_BPartner().getValue()==it.kto) {
					// kto == BP.value
				} else {
					throw new Exception("Rechnung ${invoice.rec}: BP ${it.kto} ungleich ${beleg.getC_BPartner()}")
				}
				// if ... BigDecimal getGrandTotal(boolean creditMemoAdjusted) {
				if(beleg.getGrandTotal(true).subtract(invoice.amt).abs()>0.015) {
					throw new Exception("Rechnung ${invoice.rec}: Betrag ${invoice.amt} ungleich ${beleg} diff=${beleg.getGrandTotal(true).subtract(invoice.amt)}")
				}
				def isPaid = beleg.isPaid()
				println "${CLASSNAME}:makeARReceiptAllocations Rechnung gefunden ${beleg} isPaid=${isPaid}"
				if(isPaid) {
					//addMsg("bezahlte Rechnung gefunden - ${beleg}")
					throw new Exception("Rechnung ${invoice.rec} bereits bezahlt - ${beleg}")
				}
				invoice.C_Invoice_ID = beleg.getC_Invoice_ID()
			} else if(createInvoice) {
				throw new Exception("Rechnung ${invoice.rec} nicht gefunden **createInvoice** nicht implementiert")
			} else {			
				throw new Exception("Rechnung ${invoice.rec} nicht gefunden")
			}
		}
		
		if(it.paymentList.size()==0) { 
			// kein payment Objekt: Rechnungen und Gutschriften heben sich auf
			makeAllocFromInvoices(it,C_Currency_ID)

		} else if(it.paymentList.size()==1) {
			try {
				def op = it.paymentList[0]
				def beleg = makeMPayment(op , it.C_BPartner_ID , true) // isReceipt
				if(it.invoiceList.size()==1) {
					beleg.setC_Invoice_ID( it.invoiceList[0].get('C_Invoice_ID') )
					beleg.saveEx()
					def done = beleg.processIt("CO")
					if(done) {
						println "${CLASSNAME}:makeARReceiptAllocations BP ${it.kto} 1:1 Zahlung angelegt - ${beleg}"
					} else {
						addMsg("WARN BP ${it.kto} Zahlung angelegt, aber nicht fertiggestellt - ${beleg}")
					}
					done = beleg.save()
					println "${CLASSNAME}:makeARReceiptAllocations save() : ${done}"
				} else {
					def alloc = MAllocationHdr.newInstance(this._Ctx, 0, this._TrxName)
					println "${CLASSNAME}:makeARReceiptAllocations alloc für 1 payment invoiceList=${it.invoiceList}"
					alloc.setC_Currency_ID(C_Currency_ID)
					alloc.saveEx()
					println "${CLASSNAME}:makeARReceiptAllocations alloc angelegt ${alloc}"
					BigDecimal oua = new BigDecimal(0).add(op.amt).add(op.dis==null ? 0 : op.dis) // OverUnderAmt
					it.invoiceList.each{ oi ->
						def aLine = MAllocationLine.newInstance(alloc) // Parent ctor
						aLine.setC_Payment_ID(op.get('C_Payment_ID'))
						aLine.setDocInfo(it.C_BPartner_ID,0,oi.get('C_Invoice_ID')) // 0 == no C_Order_ID
						aLine.setAmount(oi.amt)
						aLine.setDiscountAmt(oi.dis)
						oua = oua.subtract(aLine.getAmount()).subtract(aLine.getDiscountAmt())
						aLine.setOverUnderAmt(oua)
						aLine.saveEx()
						println "${CLASSNAME}:makeAPPaymentAllocations saved - ${aLine}"
					}
					// assert oua==0
					def done = alloc.processIt("CO")
					if(done) {
						println "${CLASSNAME}:makeARReceiptAllocations BP ${it.kto} n:1 Zahlung angelegt - ${alloc}"
					} else {
						addMsg("WARN BP ${it.kto} Zahlung angelegt, Allocation nicht fertiggestellt - ${alloc}")
					}
					done = alloc.save()
					println "${CLASSNAME}:makeARReceiptAllocations alloc saved=${done} - ${alloc}"	
				}
			} catch(Exception e) {
				println "${CLASSNAME}:makeARReceiptAllocations exception ${e}"
				e.printStackTrace()
				throw e
			}

		} else { // n Zahlngen
/*
com.klst.datev.OPOStoPayments:getOpos balanced? [BP:46934
, invoices:[]
, payments:[["rec":"101", "amt":69.65, "dat":2017-09-18 00:00:00.0, "kto":"BANK"]
, ["rec":"101", "amt":-69.65, "dat":2017-11-03 00:00:00.0, "kto":"BANK"]]]
 */
			try {
				it.paymentList.each{ op -> 
					def beleg = makeMPayment(op , it.C_BPartner_ID , true) // isReceipt
					def done = beleg.processIt("CO")
					println "${CLASSNAME}:makeARReceiptAllocations beleg completed=${done} - ${beleg}"
					done = beleg.save()
					println "${CLASSNAME}:makeARReceiptAllocations beleg saved=${done} - ${beleg}"
				}
				def alloc = MAllocationHdr.newInstance(this._Ctx, 0, this._TrxName)
				println "${CLASSNAME}:makeARReceiptAllocations alloc für paymentList=${it.paymentList} C_Currency_ID=${C_Currency_ID}"
				alloc.setC_Currency_ID(C_Currency_ID)
				alloc.saveEx()
				println "${CLASSNAME}:makeARReceiptAllocations alloc angelegt ${alloc}"
				
				// zuerst aLine für payments:
				BigDecimal oua = new BigDecimal(0) // OverUnderAmt
				it.paymentList.each{ op ->
					def aLine = MAllocationLine.newInstance(alloc) // Parent ctor
					aLine.setC_Payment_ID(op.get('C_Payment_ID'))
					aLine.setDocInfo(it.C_BPartner_ID,0,-1) // -1 keine Rechnung
					aLine.setAmount(op.amt)
					aLine.setDiscountAmt(op.dis)
					oua = oua.add(aLine.getAmount()).add(aLine.getDiscountAmt())
					aLine.setOverUnderAmt(oua)
					aLine.saveEx()
					println "${CLASSNAME}:makeARReceiptAllocations saved - ${aLine}"
				}
				// jetzt die rechnungen
				it.invoiceList.each{ oi ->
					def aLine = MAllocationLine.newInstance(alloc) // Parent ctor
					aLine.setDocInfo(it.C_BPartner_ID,0,oi.get('C_Invoice_ID')) 
					aLine.setAmount(oi.amt)
					oua = oua.subtract(aLine.getAmount()).subtract(aLine.getDiscountAmt())
					aLine.setOverUnderAmt(oua)
					aLine.saveEx()
					println "${CLASSNAME}:makeARReceiptAllocations saved - ${aLine}"
				}
				
				def done = alloc.processIt("CO")
				println "${CLASSNAME}:makeARReceiptAllocations alloc completed=${done} - ${alloc}"
				done = alloc.save()
				println "${CLASSNAME}:makeARReceiptAllocations alloc saved=${done} - ${alloc}"
				
			} catch(Exception e) {
				println "${CLASSNAME}:makeARReceiptAllocations exception ${e}"
				e.printStackTrace()
				throw e
			}
			
		}
		return it.paymentList.size()
	}
	
	// returns number of payments created or null if exception occurs
	def makeAllocations = { Allocations it , creditor=getPara("Creditor") , debitor=getPara("Debitor") ->
		if(!it.isBalanced()) {
			throw new OPOSexception("not balanced: ${it}")
		}
		try {
			if(it.isCreditor() && creditor=='Y') {
				return makeAPPaymentAllocations(it, getPara("CreateCreditorInvoices")=='Y')
			}
			if(it.isDebitor() && debitor=='Y') {
				return makeARReceiptAllocations(it, getPara("CreateDebitorInvoices")=='Y')
			}
		} catch(Exception e) {
//			println "${CLASSNAME}:makeAllocations catched e=${e}"
//			e.printStackTrace()
			addMsg("BP ${it.kto} " + e.getMessage())
		}
	}
	
	def getOpos = { sheet ->
		println "${CLASSNAME}:getOpos UsedRange Rows=${sheet.UsedRange.Rows.Count} Columns=${sheet.UsedRange.Columns.Count}"
		if(sheet.UsedRange.Rows.Count>1 && sheet.UsedRange.Columns.Count>=Opos.LASTCOL) {
			// OK
		} else {
			throw new Exception("UsedRange check faild: Rows=${sheet.UsedRange.Rows.Count}/expected>1 Columns=${sheet.UsedRange.Columns.Count}/expected>=${Opos.LASTCOL}")
		}

		def range = sheet.UsedRange
		Opos.HEADER.each { k, v -> 
			//println "${CLASSNAME}:getOpos Range check ${k}"
			def val = range.Item(2,k.intValue()).Value
			//println "${CLASSNAME}:getOpos Range val=${val} ${k.class}"
			if(val==v) {
				println "${CLASSNAME}:getOpos Range check OK '${v}'"		
			} else {
				throw new Exception("HEADER check faild: expected '${v}' found '${val}'")
			}
		}
		
		def lastKto = '-1'
		def lastRec = '-1'
		println "${CLASSNAME}:getOpos Konto=${lastKto} Rechnungs-Nr.=${lastRec}"
		Allocations alloc = null
		def counter = [0,0,0,0] // #debitor , #kreditor , #ARR == "Zahlungseingang" , #APP == "Zahlungsausgang" 
		Opos opos = new Opos(range)
		for( r=3; r<=range.Rows.Count; r++) {
			def kto = opos.getDoubleOrStringToString(range.Item(r, Opos.KONTO).Value)
			def rec = opos.getDoubleOrStringToString(range.Item(r, Opos.RECHNUNGSNR).Value)
			def saldo = range.Item(r, Opos.SALDO).Value
			def ausgl = range.Item(r, Opos.AUSGLAMN).Value
			if(opos.getValue(r,Opos.BUCHDAT)==null) { // darf nicht passeiern, eof
				println "${CLASSNAME}:getOpos eof"
				return
			}
			//println "${CLASSNAME}:getOpos kto='${kto}' rec='${rec}' ???"
			if(kto==null || kto=='0') {
				kto = lastKto
				if(alloc.isDebitor()) {
					if(lastRec==rec) { // gleiche Ausgangsrechnung
						try {
							alloc.addPayment(opos.makePayment(kto,r))
						} catch(OPOSexception e) {
							alloc.addInvoice(opos.makeInvoice(kto,r))
						}
					} else { // neue Rechnung des gleichen BP
						try {
							if(saldo==null) { // ein neuer OPOS - der alte muss ausbalanciert sein
								println "${CLASSNAME}:getOpos balanced? ${alloc}"
								def numOfARR = makeAllocations(alloc)
								counter[2] = counter[2] + (numOfARR==null ?  0 : numOfARR)
								alloc = new Allocations()
								alloc.setDebitor(kto)
							} else {
								println "${CLASSNAME}:getOpos NOT BALANCED saldo=${saldo} == ${alloc.getSaldo()}"
							}
						} catch(OPOSexception e) {
							println "${CLASSNAME}:getOpos neue Rechnung des gleichen BP exception=${e.getMessage()}"
						}
						try {
							alloc.addInvoice(opos.makeInvoice(kto,r))
							println "${CLASSNAME}:getOpos neue Rechnung des gleichen BP saldo=${saldo} rec=${rec} ausgl='${ausgl}' "
						} catch(OPOSexception e) {
							println "${CLASSNAME}:getOpos exception=${e.getMessage()} versuche payment..."
							alloc.addPayment(opos.makePayment(kto,r))
						}
					}
				} else { // bei Lieferanten
					if(lastRec==rec) { // gleiche Eingangsrechnung
						try {
							alloc.addPayment(opos.makePayment(kto,r))
						} catch(OPOSexception e) {
							alloc.addInvoice(opos.makeInvoice(kto,r))
						}
					} else { // neue Rechnung des gleichen Lieferanten
						try {
							if(saldo==null) { // ein neuer OPOS - der alte muss ausbalanciert sein
								println "${CLASSNAME}:getOpos balanced? ${alloc}"
								def numOfAPP = makeAllocations(alloc)
								counter[3] = counter[3] + (numOfAPP==null ?  0 : numOfAPP)
								alloc = new Allocations()
								alloc.setCreditor(kto)
							} else {
								println "${CLASSNAME}:getOpos NOT BALANCED saldo=${saldo} == ${alloc.getSaldo()}"
							}
						} catch(OPOSexception e) {
							println "${CLASSNAME}:getOpos neue Rechnung des gleichen BP exception=${e.getMessage()}"
						}
						try {
							alloc.addInvoice(opos.makeInvoice(kto,r))
						} catch(OPOSexception e) {
							alloc.addPayment(opos.makePayment(kto,r))
						}
//						println "${CLASSNAME}:getOpos BP=${alloc.kto} invoices=${alloc.invoiceList} payments=${alloc.paymentList}"
					}

				}
			} else { // neues Konto/BP
				if(alloc!=null) try {
					println "${CLASSNAME}:getOpos BP balanced? ${alloc}"
					def numOfPay = makeAllocations(alloc)
					if(alloc.isDebitor()) {
						counter[0] = counter[0] + 1
						counter[2] = counter[2] + (numOfPay==null ?  0 : numOfPay)
					} else {
						counter[1] = counter[1] + 1
						counter[3] = counter[3] + (numOfPay==null ?  0 : numOfPay)
					}
				} catch(OPOSexception e) {
					addMsg("exception " + e.getMessage())
				}
				alloc = new Allocations()
				println "${CLASSNAME}:getOpos neues Konto/BP=${kto} ausgl='${ausgl}' BESCHRIFTUNG=${opos.getValue(r,Opos.BESCHRIFTUNG)} "
				try {
					alloc.setDebitor(kto)
				} catch(OPOSexception e) {
					println "${CLASSNAME}:getOpos ${e.getMessage()} ** Lieferant **"
					alloc.setCreditor(kto)
				}
				try {
					alloc.addInvoice(opos.makeInvoice(kto,r))
				} catch(OPOSexception e) {
					println "${CLASSNAME}:getOpos exception=${e.getMessage()} - trying payment"
					alloc.addPayment(opos.makePayment(kto,r))
				}
			}
			
			lastKto = kto
			lastRec = rec
		} //for
		if(alloc!=null) {
			println "${CLASSNAME}:getOpos balanced? ${alloc}"
			def numOfPay = makeAllocations(alloc)
			if(alloc.isDebitor()) {
				counter[0] = counter[0] + 1
				counter[2] = counter[2] + (numOfPay==null ?  0 : numOfPay)
			} else {
				counter[1] = counter[1] + 1
				counter[3] = counter[3] + (numOfPay==null ?  0 : numOfPay)
			}
		}
		//addMsg("Summen (#debitor, #creditor, #ARR/Zahlungseingang, #APP/Zahlungsausgang)=${counter}")
		if(getPara("Debitor")=='Y') {
			addMsg("Anzahl debitor:${counter[0]} bearbeitet, ${counter[2]} Zahlungen erstellt")		
		}
		if(getPara("Creditor")=='Y') {
			addMsg("Anzahl creditor:${counter[1]} bearbeitet, ${counter[3]} Zahlungen erstellt")		
		}
	}

	@Override
	public Object run() {
		println "${CLASSNAME}:run"
		def excel = getExcel()
		def sheet = null
		if(isProcess()) try {
			sheet = getSheet(excel)
		} catch(ComFailException e) {
			return e.getMessage()
		} else {
			println "${CLASSNAME}:run noProcess"
			sheet = getSheet(excel,"${XLSDIR_TEST}OPOS-2018-01.xlsx")
		}
		addMsg("excel gelesen - ${sheet.UsedRange.Rows.Count} Zeilen")
		try {
			getOpos(sheet)			
		} catch(Exception e) {
			return e.getMessage()
		}
		println "${this.msg}"
		excel.Workbooks.Close()
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

class OPOSexception extends GroovyException {
	public OPOSexception(String message) {
		super(message)
	}
}

class SKR03 extends Script {
	
	def CLASSNAME = this.getClass().getName()
	
	public SKR03() {
		println "${CLASSNAME}:ctor"
	}

	public SKR03(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}
	
	static final      KTO_FORMAT =  "####"
	static final PERS_KTO_FORMAT = "#####"

	static def isDebitor = { it ->
		if(it.class.getName()=='java.lang.String') {
			it = Integer.parseInt(it)
		}
		return it>=10000 && 69999>=it // 10000-69999 : Debitoren/Kunden
	}
	
	static def isCreditor = { it ->
		if(it.class.getName()=='java.lang.String') {
			it = Integer.parseInt(it)
		}
		return it>=70000 && 99999>=it // 70000-99999 : Kreditoren/Lieferanten
	}

	// Andere Anlagen, Betriebs- und Geschäftsausstattung
	//      0400 Betriebsausstattung
	//      0410 Geschäftsausstattung
	//      0420 Büroeinrichtung
	//      0430 Ladeneinrichtung
	//      0440 Werkzeuge
	//      0450 Einbauten in fremde Grundstücke
	//      0460 Gerüst- und Schalungsmaterial
	// ...
	//      0970 Sonstige Rückstellungen
	//      0977 Rückstellungen für Abschluss- und Prüfungskosten
	//      1590 Durchlaufende Posten
	//      2309 Sonstige Aufwendungen unregelmäßig (WriteOff/Abschreibung)
	// Fremdleistungen
	//   AV 3123 Sonstige Leistungen eines im anderen EU-Land ansässigen Unternehmers 19 % Vorsteuer und 19 % Umsatzsteuer
	// Aufwendungen für Roh-, Hilfs- und Betriebsstoffe und für bezogene Waren 3200-3969 :
	//   AV 3400-09 Wareneingang 19 % Vorsteuer
	//   AV 3425-29 Innergemeinschaftlicher Erwerb 19 % Vorsteuer und 19 % Umsatzsteuer
	// Skonti
	//    S 3730 Erhaltene Skonti
	// S/AV 3736 Erhaltene Skonti 19 % Vorsteuer
	//    R 3749
	// Boni
	// AV 3750-51 Erhaltene Boni 7 % Vorsteuer
	//   AV 3760-61 Erhaltene Boni 19 % Vorsteuer
	//      3769 Erhaltene Boni
	// ---
	// Sonstige betriebliche Aufwendungen und Abschreibungen 4200-4985 :
	//      4210 Miete (unbewegliche Wirtschaftsgüter)
	//      4240 Gas, Strom, Wasser
	//      4730 Ausgangsfrachten
	//      4806 Wartungskosten für Hard- und Software
	// 310x Fremdleistungen
	//    S 8730 Gewährte Skonti
	// ...
	// Umsatzerlöse
	//   AM 8120 Steuerfreie Umsätze § 4 Nr. 1a UStG
	// U AM 8125 Steuerfreie innergemeinschaftliche Lieferungen § 4 Nr. 1b UStG
	//   AM 8300-09 Erlöse 7 % USt
	// U AM 8310-14	Erlöse aus im Inland steuerpflichtigen EU-Lieferungen 7 % USt
	// U AM 8315-19	Erlöse aus im Inland steuerpflichtigen EU-Lieferungen 19 % USt
	//      8320-29 Erlöse aus im anderen EU-Land steuerpflichtigen Lieferungen3)
	// ...
	//   AM 8400-10 Erlöse 19 % USt
	//    R 8749
	static def isAusstattung = { it ->
		return it>=400 && it<=490
	}
	static def isFremdleistung = { it ->
		return it>=3100 && it<=3165
	}
	static def isAufwendung = { it ->
		return (it>=3200 && it<=3429) || (it>=4200 && it<=4985)
	}
	static def isBonus = { it ->
		return (it>=3750 && it<=3769)
	}
	static def isDiverses = { it ->
		if(it.class==String && it=="div.") {
			return true
		}
		return (it>=970 && it<=979) // Rückstellungen
	}

	// für Ausgangsrechnungen
	static def isErloesKto = { it ->
		if(it.class==String && it=="div.") {
			return true
		}
		return (it>=8400 && it<=8410) || (it>=8300 && it<=8309) || (it>=8310 && it<=8329) ||it==8120 ||it==8125
	}
	
	// für Zahlungen  
	static def isSkonto = { it ->
		return (it>=8730 && it<=8749) || (it>=3730 && it<=3749)
	}
	static def isBank = { it ->
		return (it>=1100 && it<=1130) || (it>=1200 && it<=1250) // Postbank || Bank 
	}
	static def isWriteOff = { it ->
		return it==2309
	}

	@Override
	public Object run() {
	}

}

class Opos extends Script {
	
	def CLASSNAME = this.getClass().getName()

	static final BANK = "BANK"
	static final SKONTO = "SKONTO"
	static final WRITEOFF = "WriteOff"
	
	static final KONTO          = 2 // pers.Kto
	static final BESCHRIFTUNG   = 3 // kto
	static final RECHNUNGSNR    = 4
	static final BUCHDAT        = 5 // java.util.Date
	static final FAELLIGDAT     = 6 // java.util.Date or null
	static final SOLL           = 7
	static final HABEN          = 8
	static final SALDO          = 9
	static final SHSALDO        =10 // S H
	static final GEGENKTO       =11
	static final RAFFUNG        =12 // wenn R => GEGENKTO = div.
	static final FAELLIG        =13
	static final AUSGLAMN       =14 // M: manuell , A: automatisch , N: KontenNullsaldenlauf
	static def mapAusgl = ["M":"manuell","A":"automatisch","N":"via KontenNullsaldenlauf"]
	static final BELEGFELD2     =15 // TTMMYY ? Fälligkeit
	static final KZ             =16
	static final BUTEXT         =17 // ab hier andere Reihenfolge im DEZ 2017
	static final KOST1          =18
	static final KOST2          =19
	static final USTVH          =20 // z.B. 19 7 "div."
	static final STAPELNR       =21
	static final BSNR           =22 // last used col
	// ...
	static final LASTCOL = Opos.BSNR
	static def HEADER = [1:"BL"
, 2:"Konto"
, 3:"Beschriftung"
, 4:"Rechnungs-Nr."
, 5:"Datum"
, 6:"Fälligkeit"
, 7:"Betrag Soll"
, 8:"Betrag Haben"
, 9:"Saldo"
,10:"S/H Saldo"
,11:"Gegenkonto"
,12:"R"
,13:"Ausgl."
,14:"fällig"
,15:"Belegfeld 2"
,16:"Kz"
,17:"Buchungstext"
,18:"KOST1"
,19:"KOST2"
,20:"USt%"
,21:"Stapel-Nr."
,22:"BSNr."
]
	
	def range = null
	def SKR = SKR03
	
	public Opos(range) {
		println "${CLASSNAME}:ctor"
		this.range = range
	}

	public Opos(Binding binding) {
		super(binding)
		println "${CLASSNAME}:ctor binding"
	}
	
	def getValue = { row , col ->
		return this.range.Item(row,col).Value
	}
		
	def toTimestamp = { it, format="EEE MMM dd hh:mm:ss zzz yyyy" ->
		if(it==null) {
			return null
		}
		if(it.class.getName()=='java.util.Date') {
			return new java.sql.Timestamp(it.getTime())
		}
		// das default format gilt für csv
		SimpleDateFormat fromFormat = new SimpleDateFormat(format)
		Date date2 = fromFormat.parse(it)
		return new java.sql.Timestamp(date2.getTime())
	}

	// to get RECHNUNGSNR or KONTO
	def getDoubleOrStringToString = { it
		if(it==null) {
			return null
		}
		if(it.class.getName()=='java.lang.Double') { // bei .csv
			return new DecimalFormat("#######").format(it)
		} else {
			return it.trim()
		}
	}
	
	// Aufruf : def invoice = opos.makeInvoice(kto,r)
	def makeInvoice = { kto, r ->
		def invoice = [:]
		def soll = range.Item(r, SOLL).Value
		def haben = range.Item(r, HABEN).Value
		def butext = range.Item(r, BUTEXT).Value
		def recNr = range.Item(r, RECHNUNGSNR).Value
		//println "${CLASSNAME}:makeInvoice recNr=${recNr} ${recNr.class}"
		invoice.rec = getDoubleOrStringToString(recNr)
		//println "${CLASSNAME}:makeInvoice invoice.rec=${invoice.rec} ${invoice.rec.class}"
		invoice.amt = soll!=null ? soll : 0 - haben
		invoice.kto = range.Item(r, GEGENKTO).Value
		def buchdat = range.Item(r,BUCHDAT).Value
		//println "${CLASSNAME}:makeInvoice buchdat=${buchdat} ${buchdat.class}"
		invoice.dat = toTimestamp(buchdat,"dd.MM.yyyy")
		invoice.txt = ""
		invoice.ust = range.Item(r,USTVH).Value
		if(SKR03.isDebitor(kto)) { // Ausgangsrechnungen : das Gegenkoto muss ein ErlösKto sein
			if(SKR.isErloesKto(invoice.kto)) {
				// OK
			} else if(SKR.isBonus(invoice.kto)) { // eigentlich unsauber, denn Bonus gibt es nicht vom Debitor
				invoice.txt = butext
				println "${CLASSNAME}:makeInvoice butext=${butext}"
			} else {
				throw new OPOSexception("Zeile ${r}: BP ${kto} Re ${invoice.rec} ist keine Ausgangsrechnung, wg gegenkto ${invoice.kto}")
			}
		} else { // Kreditoren/Lieferanten - > Eingangsrechnungen 
			if(SKR.isDiverses(invoice.kto) || SKR.isAusstattung(invoice.kto) || SKR.isAufwendung(invoice.kto) || SKR.isFremdleistung(invoice.kto)) {
				invoice.txt = butext
			} else if(SKR.isBonus(invoice.kto)) {
				invoice.txt = butext
			} else {
				throw new OPOSexception("Zeile ${r}: BP ${kto} Re ${invoice.rec} ist keine Eingangsrechnung, wg gegenkto ${invoice.kto}")
			}
		}
		println "${CLASSNAME}:makeInvoice ${invoice}"
		return invoice
	}
	
	// Aufruf : def payment = opos.makePayment(kto,r)
	def makePayment = { kto, r ->
		def payment = [:]
		def soll = range.Item(r, SOLL).Value
		def haben = range.Item(r, HABEN).Value
		def recNr = range.Item(r, RECHNUNGSNR).Value
		payment.rec = getDoubleOrStringToString(recNr)
		payment.amt = haben!=null ? haben : 0 - soll
		def gegen = range.Item(r, GEGENKTO).Value
		payment.dat = toTimestamp(range.Item(r,BUCHDAT).Value,"dd.MM.yyyy")
		if(gegen.class==String && gegen=="div." || SKR.isWriteOff(gegen)) {
			payment.kto = WRITEOFF
		} else if(SKR.isBank(gegen)) {
			payment.kto = BANK
		} else if(SKR.isSkonto(gegen)) {
			payment.kto = SKONTO
		} else {
			throw new OPOSexception("Zeile ${r}: BP ${kto} Re ${payment.rec} ist keine Zahlung, wg gegenkto ${gegen}")
		}
		println "${CLASSNAME}:makePayment ${payment}"
		return payment
	}

	@Override
	public Object run() {
	}

}

class Allocations extends Script {
	
	def CLASSNAME = this.getClass().getName()
	
	String kto = null // personenKto
	def invoiceList = []
	def paymentList = []
	
	public Allocations() {
		println "${CLASSNAME}:ctor"
	}

	public Allocations(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}

	def addInvoice = { it ->
		invoiceList.add(it)
	}
	
	def addPayment = { it ->
		paymentList.add(it)
	}
	
	def isDebitor = { it=this.kto.toInteger() ->
		return SKR03.isDebitor(it)
	}
	
	public void setDebitor(Object it)	{
		if(isDebitor(it)) {
			if(it.class.getName()=='java.lang.String') {
				this.kto = it
			} else {
				this.kto = new DecimalFormat(SKR03.PERS_KTO_FORMAT).format(it)
			}
		} else {
			throw new OPOSexception("BP ${it} ist kein Debitor")
		}
	}
	
	def isCreditor = { it=this.kto.toInteger() ->
		return SKR03.isCreditor(it)
	}

	public void setCreditor(Object it)	{
		if(isCreditor(it)) {
			if(it.class.getName()=='java.lang.String') {
				this.kto = it
			} else {
				this.kto = new DecimalFormat(SKR03.PERS_KTO_FORMAT).format(it)
			}
		} else {
			throw new OPOSexception("BP ${it} ist kein Kreditor")
		}
	}
	
	public String getBP() {
		return this.kto
	}

	def getSkontoList = {
		def sl = []
		this.paymentList.each{ 
			if(it.kto.class==String && it.kto == Opos.SKONTO) {
				sl.add(it)
			}
		}
		return sl
	}
	
	def getSaldo = {
		BigDecimal saldo = new BigDecimal(0)
		this.invoiceList.each{
			saldo = saldo.add(it.amt)
		}
		this.paymentList.each{
			saldo = saldo.subtract(it.amt)
		}
		return saldo
	}
	
	def isBalanced = {
		BigDecimal saldo = this.getSaldo()
		def res = saldo.abs()<=0.005
		if(res) {
			def skontoList = getSkontoList()
			println "${CLASSNAME}:isBalanced skonti=${skontoList}"
			skontoList.each{ 
				def pb=-1
				for(int p=0;p<paymentList.size();p++) {
					if(paymentList[p].kto==Opos.BANK && it.rec==paymentList[p].rec) {
						paymentList[p].dis = it.amt
						println "${CLASSNAME}:isBalanced p=${paymentList[p]}"
						pb = p
					}
				}
				if(pb==-1) {
					throw new OPOSexception("kein p zu ${it} gefunden")
				}
				paymentList = paymentList.minus(skontoList)
				println "${CLASSNAME}:isBalanced payments:${paymentList}"
			}
		}
		return res
	}

	public String toString() {
		return "[BP:${kto}, invoices:${invoiceList}, payments:${paymentList}]"
	}
	
	@Override
	public Object run() {
	}

}
