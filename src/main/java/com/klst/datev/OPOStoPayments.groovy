// Feat #1417 - Bezahlte Rechnungen datev->AD
package com.klst.datev

import groovy.lang.Binding
import groovy.lang.Script
import java.text.DecimalFormat
import java.text.ParsePosition
import java.text.SimpleDateFormat
import java.util.Properties

import org.codehaus.groovy.scriptom.ActiveXObject
import com.jacob.com.ComFailException

class OPOStoPayments extends Script {
	
	def CLASSNAME = this.getClass().getName()

	public CreateITDZOrders() {
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
	def pXls = null
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
 P    A_AD_PInstance_ID --- hiermit komme ich an AD_Process_ID select * from AD_PInstance ==> class MPInstance
 P    A_Table_ID
  */
			 
			this._Ctx = A_Ctx
			this._TrxName = A_TrxName
			println "${CLASSNAME}:isProcess A_AD_PInstance_ID: ${A_AD_PInstance_ID}"
//			this._pi = new MPInstance(this._Ctx, A_AD_PInstance_ID, null)
//			println "${CLASSNAME}:isProcess ${this._pi}"
//			MPInstancePara[] paraList = this._pi.getParameters()
//			for (para in paraList) {
//				println "${CLASSNAME}:isProcess ${para.getParameterName()} ${para.getP_String()}"
//				if(para.getParameterName()=="FileName") {
//					this.pXls = para.getP_String()
//				}
//			}
		}
		return ret
	}

	def envGetContext = { it , WindowNo=this.getProperty("A_WindowNo") -> //.toInteger() ->
		String context = "${WindowNo}|${it}"
		println "${CLASSNAME}:envGetContext result:"+ctx().get(context)
		return ctx().get(context)
	}

	private static final ACTIVEX_EXCEL = "Excel.Application"
	def getExcel = { ->
		def objExcel = new ActiveXObject(ACTIVEX_EXCEL)
		println "${CLASSNAME}:getExcel Version ${objExcel.Version}"
		return objExcel
	}
	
	private static final CSV_EXT = ".csv"
	private static final XLSDIR_TEST = "C:\\proj\\minhoff\\DATEV\\"
	def getSheet = { file=this.pXls ->
		println "${CLASSNAME}:getSheet ${file}"
		def excel = getExcel()
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

	def toTimestamp = { it, format="EEE MMM dd hh:mm:ss zzz yyyy" ->
		if(it==null) {
			return null
		}
		return new java.sql.Timestamp(it.getTime())
	}
	// columns
	private static final KONTO          = 2 // pers.Kto
	private static final BESCHRIFTUNG   = 3 // kto
	private static final RECHNUNGSNR    = 4
	private static final BUCHDAT        = 5 // java.util.Date
	private static final FAELLIGDAT     = 6 // java.util.Date or null
	private static final SOLL           = 7
	private static final HABEN          = 8
	private static final SALDO          = 9
	private static final SHSALDO        =10 // S H
	private static final GEGENKTO       =11
	private static final RAFFUNG        =12 // wenn R => GEGENKTO = div.
	private static final FAELLIG        =13
	private static final AUSGLAMN       =14 // M: manuell , A: automatisch , N: KonteNnullsaldenlauf
	def mapAusgl = ["M":"manuell","A":"automatisch","N":"via KonteNnullsaldenlauf"]
	// ...
	private static final ANSRECHPARTNER =43 // last col
	
	def getValue = { range, row , col->
		return range.Item(row,col).Value
	}
	
	def getOpos = { sheet , orderList ->
		println "${CLASSNAME}:getOpos UsedRange Rows=${sheet.UsedRange.Rows.Count} Columns=${sheet.UsedRange.Columns.Count}"
		if(sheet.UsedRange.Rows.Count>1 && sheet.UsedRange.Columns.Count==ANSRECHPARTNER) {
			// OK
		} else {
			throw new Exception("UsedRange check faild: Rows=${sheet.UsedRange.Rows.Count}/expected>1 Columns=${sheet.UsedRange.Columns.Count}/expected=${ANSRECHPARTNER}")
		}
		def range = sheet.UsedRange
		def lastKto = range.Item(2,KONTO).Value
		def lastRec = range.Item(2,RECHNUNGSNR).Value
		println "${CLASSNAME}:getOpos Konto=${lastKto} Rechnungs-Nr.=${lastRec}"
		if("Konto"==lastKto && "Rechnungs-Nr."==lastRec) {
			// OK
		} else {
			throw new Exception("Row 2 check faild: expected 'Konto'='${lastKto}' AND 'Rechnungs-Nr.'='${lastRec}'")
		}
		Allocations alloc = null
		def rechnung = [:]
		for( r=3; r<=range.Rows.Count; r++) {
			def kto = range.Item(r, KONTO).Value
			def rec = range.Item(r, RECHNUNGSNR).Value
			def gegenkto = range.Item(r, GEGENKTO).Value
			def soll = range.Item(r, SOLL).Value
			def haben = range.Item(r, HABEN).Value
			def saldo = range.Item(r, SALDO).Value
			def ausgl = range.Item(r, AUSGLAMN).Value
			def buchdat = toTimestamp(range.Item(r,BUCHDAT).Value)
			if(buchdat==null) { // darf nicht passeiern, eof
				println "${CLASSNAME}:getOpos eof"
				return
			}
			if(kto==null) {
				kto = lastKto
				if(lastRec==rec) { // gleiche Rechnung
					def payment = alloc.makePayment(rec,buchdat,soll,haben,gegenkto)
					println "${CLASSNAME}:getOpos payment=${payment}"
					alloc.addPayment(payment)
				} else { // neue Rechnung des gleichen BP
					def invoice = alloc.makeInvoice(rec,buchdat,soll,haben)
					println "${CLASSNAME}:getOpos neue Rechnung des gleichen BP saldo=${saldo} rec=${rec} ausgl='${ausgl}' "
					if(saldo==null) { // ein neuer OPOS - der alte muss ausbalanciert sein
						assert alloc.isBalanced()
						// TODO alloc in AD anlegen
						alloc = new Allocations()
						alloc.setDebitor(kto)
					} else {
						// 
						println "${CLASSNAME}:getOpos saldo=${saldo} == ${alloc.getSaldo()}"
					}
					alloc.addInvoice(invoice)
					println "${CLASSNAME}:getOpos BP=${alloc.kto} ${alloc.invoiceList} ${alloc.paymentList}"
				}
			} else { // neues Konto/BP
				alloc = new Allocations()
				alloc.setDebitor(kto)
				println "${CLASSNAME}:getOpos neues Konto/BP=${alloc.kto} ausgl='${ausgl}' BESCHRIFTUNG=${getValue(range,r,BESCHRIFTUNG)} "
				try {
					def invoice = alloc.makeInvoice(rec,buchdat,soll,haben,gegenkto)
					alloc.addInvoice(invoice)
				} catch(Exception e) {
					println "${CLASSNAME}:getOpos exception=${e.getMessage()}"
					def payment = alloc.makePayment(rec,buchdat,soll,haben,gegenkto)
					alloc.addPayment(payment)
				} 
				println "${CLASSNAME}:getOpos BP=${alloc.kto} invoices=${alloc.invoiceList} payments=${alloc.paymentList}"
			}
			
			if(saldo!=null) {
				if(alloc.isBalanced()) {
					println "${CLASSNAME}:getOpos isBalanced"
				}
			}
			lastKto = kto
			lastRec = rec
		} //for
		return orderList
	}

	@Override
	public Object run() {
		println "${CLASSNAME}:run"
		if(isProcess()) try {
			def sheet = getSheet()
			addMsg("excel gelesen - ${sheet.UsedRange.Rows.Count} Zeilen")
//		    def orderList = []	// empty
//			getOpos(sheet,orderList)
//			addMsg("${orderList.size} Order.")
		} catch(ComFailException e) {
			return e.getMessage()
		} else {
			println "${CLASSNAME}:run noProcess"
			def sheet = getSheet("${XLSDIR_TEST}OPOS ausgeglichen 11_2017 ${CSV_EXT}")
			addMsg("excel gelesen - ${sheet.UsedRange.Rows.Count} Zeilen")
		    def orderList = []	// empty
			getOpos(sheet,orderList)
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
class Allocations extends Script {
	
	def CLASSNAME = this.getClass().getName()
	
	def kto = null // personenKto
	def invoiceList = []
	def paymentList = []
	
	public Allocations() {
		println "${CLASSNAME}:ctor"
	}

	public Allocations(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}

	static def isErloesKto = { it ->
		return it==8400 ||it==8300 ||it==8120 ||it==8125 ||it==8315 ||it==8310 ||it=='div.'
	}

	def makeInvoice = { rec , dat , soll , haben , gegen=null ->
		def invoice = [:]
		if(gegen==null || isErloesKto(gegen)) {
			invoice.rec = new DecimalFormat("#######").format(rec)
			invoice.amt = soll!=null ? soll : 0 - haben
			invoice.dat = dat
		} else {
			throw new Exception("BP ${this.kto} Re ${rec} ist keine Rechnung, wg gegenkto ${gegen}")
		}
		return invoice
	}

	def makePayment = { rec , dat , soll , haben , gegen ->
		def payment = [:]
		if(gegen=="div.") {
			payment.kto = gegen
		} else if(gegen>=1200 && gegen<=1250) {
			payment.kto = 'Bank'
		} else if(gegen==8736) {
			payment.kto = 'Skonto'
		} else {
			throw new Exception("BP ${this.kto} Re ${rec} ist keine Zahlung, wg gegenkto ${gegen}")
		}
		payment.rec = new DecimalFormat("#######").format(rec)
		payment.amt = haben!=null ? haben : 0 - soll
		payment.dat = dat
		return payment
	}
	
	def addInvoice = { it ->
		invoiceList.add(it)
	}
	
	def addPayment = { it ->
		paymentList.add(it)
	}
	
	def setDebitor = { it ->
		if(it<10000 || 69999<it) { // 10000-69999 : Debitoren
			throw new Exception("BP ${it} ist kein Debitor")
		}
		this.kto = new DecimalFormat("#####").format(it)
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
		return saldo.abs()<=0.005
	}

	@Override
	public Object run() {
		return null;
	}

	static main(args) {
	}
	
}
		
	
	
