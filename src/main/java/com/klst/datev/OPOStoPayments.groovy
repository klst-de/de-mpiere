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

	def getOpos = { sheet ->
		println "${CLASSNAME}:getOpos UsedRange Rows=${sheet.UsedRange.Rows.Count} Columns=${sheet.UsedRange.Columns.Count}"
		if(sheet.UsedRange.Rows.Count>1 && sheet.UsedRange.Columns.Count==Opos.ANSRECHPARTNER) {
			// OK
		} else {
			throw new Exception("UsedRange check faild: Rows=${sheet.UsedRange.Rows.Count}/expected>1 Columns=${sheet.UsedRange.Columns.Count}/expected=${ANSRECHPARTNER}")
		}
		def range = sheet.UsedRange
		def lastKto = range.Item(2,Opos.KONTO).Value
		def lastRec = range.Item(2,Opos.RECHNUNGSNR).Value
		println "${CLASSNAME}:getOpos Konto=${lastKto} Rechnungs-Nr.=${lastRec}"
		if("Konto"==lastKto && "Rechnungs-Nr."==lastRec) {
			// OK
		} else {
			throw new Exception("Row 2 check faild: expected 'Konto'='${lastKto}' AND 'Rechnungs-Nr.'='${lastRec}'")
		}
		Allocations alloc = null
		Opos opos = new Opos(range)
		for( r=3; r<=range.Rows.Count; r++) {
			def kto = range.Item(r, Opos.KONTO).Value
			def rec = range.Item(r, Opos.RECHNUNGSNR).Value
			def saldo = range.Item(r, Opos.SALDO).Value
			def ausgl = range.Item(r, Opos.AUSGLAMN).Value
			if(opos.getValue(r,Opos.BUCHDAT)==null) { // darf nicht passeiern, eof
				println "${CLASSNAME}:getOpos eof"
				return
			}
			if(kto==null) {
				kto = lastKto
				if(alloc.isDebitor()) {
					if(lastRec==rec) { // gleiche Ausgangsrechnung
						alloc.addPayment(opos.makePayment(kto,r))
					} else { // neue Rechnung des gleichen BP
						if(saldo==null) { // ein neuer OPOS - der alte muss ausbalanciert sein
							println "${CLASSNAME}:getOpos balanced? BP=${alloc.kto} invoices=${alloc.invoiceList} payments=${alloc.paymentList}"
							assert alloc.isBalanced()
							// TODO alloc in AD anlegen
							alloc = new Allocations()
							alloc.setDebitor(kto)
						} else {
							println "${CLASSNAME}:getOpos NOT BALANCED saldo=${saldo} == ${alloc.getSaldo()}"
						}
						try {
							alloc.addInvoice(opos.makeInvoice(kto,r))
							println "${CLASSNAME}:getOpos neue Rechnung des gleichen BP saldo=${saldo} rec=${rec} ausgl='${ausgl}' "
						} catch(Exception e) {
							println "${CLASSNAME}:getOpos exception=${e.getMessage()} versuche payment..."
							alloc.addPayment(opos.makePayment(kto,r))
						} 
					}
				} else { // bei Lieferanten
					if(lastRec==rec) { // gleiche Eingangsrechnung
						try {
							alloc.addPayment(opos.makePayment(kto,r))
						} catch(Exception e) {
							alloc.addInvoice(opos.makeInvoice(kto,r))
						}
					} else { // neue Rechnung des gleichen Lieferanten
						if(saldo==null) { // ein neuer OPOS - der alte muss ausbalanciert sein
							println "${CLASSNAME}:getOpos balanced? BP=${alloc.kto} invoices=${alloc.invoiceList} payments=${alloc.paymentList}"
							assert alloc.isBalanced()
							// TODO alloc in AD anlegen
							alloc = new Allocations()
							alloc.setCreditor(kto)
						} else {
							println "${CLASSNAME}:getOpos NOT BALANCED saldo=${saldo} == ${alloc.getSaldo()}"
						}
						try {
							alloc.addInvoice(opos.makeInvoice(kto,r))
						} catch(Exception e) {
							alloc.addPayment(opos.makePayment(kto,r))
						} 
//						println "${CLASSNAME}:getOpos BP=${alloc.kto} invoices=${alloc.invoiceList} payments=${alloc.paymentList}"
					}

				}
			} else { // neues Konto/BP
				if(alloc!=null) {
					println "${CLASSNAME}:getOpos balanced? BP=${alloc.kto} invoices=${alloc.invoiceList} payments=${alloc.paymentList}"
					assert alloc.isBalanced()
					// TODO alloc in AD anlegen
				}
				alloc = new Allocations()
				println "${CLASSNAME}:getOpos neues Konto/BP=${kto} ausgl='${ausgl}' BESCHRIFTUNG=${opos.getValue(r,Opos.BESCHRIFTUNG)} "
				try {
					alloc.setDebitor(kto)
				} catch(Exception e) {
					println "${CLASSNAME}:getOpos ${e.getMessage()} ** Lieferant **"
					alloc.setCreditor(kto)
				}
				try {
					alloc.addInvoice(opos.makeInvoice(kto,r))
				} catch(Exception e) {
					println "${CLASSNAME}:getOpos exception=${e.getMessage()} - trying payment"
					alloc.addPayment(opos.makePayment(kto,r))
				} 
			}
			
			lastKto = kto
			lastRec = rec
		} //for
		return alloc
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
			getOpos(sheet)
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
		return it>=10000 && 69999>=it // 10000-69999 : Debitoren/Kunden
	}
	
	static def isCreditor = { it ->
		return it>=70000 && 99999>=it // 70000-99999 : Kreditoren/Lieferanten
	}

	//      0970 Sonstige Rückstellungen
	// Aufwendungen für Roh-, Hilfs- und Betriebsstoffe und für bezogene Waren 3200-3969 :
	//   AV 3400-09 Wareneingang 19 % Vorsteuer
	//   AV 3425-29 Innergemeinschaftlicher Erwerb 19 % Vorsteuer und 19 % Umsatzsteuer
	// S/AV 3736 Erhaltene Skonti 19 % Vorsteuer
	//   AV 3760-61 Erhaltene Boni 19 % Vorsteuer
	// ---
	// Sonstige betriebliche Aufwendungen und Abschreibungen 4200-4985 :
	//      4210 Miete (unbewegliche Wirtschaftsgüter)
	//      4240 Gas, Strom, Wasser
	//      4730 Ausgangsfrachten
	//      4806 Wartungskosten für Hard- und Software
	// 310x Fremdleistungen
	//    S 8730 Gewährte Skonti
	// ...
	//    R 8749
	static def isFremdleistung = { it ->
		return it>=3100 && it<=3109
	}
	static def isAufwendung = { it ->
		return (it>=3200 && it<=3969) || (it>=4200 && it<=4985)
	}
	
	// für Ausgangsrechnungen
	static def isErloesKto = { it ->
		return it==8400 ||it==8300 ||it==8120 ||it==8125 ||it==8315 ||it==8310 ||it=='div.'
	}
	
	// für Zahlungen  
	static def isSkonto = { it ->
		return it>=8730 && it<=8749
	}
	static def isBank = { it ->
		return (it>=1100 && it<=1130) || (it>=1200 && it<=1250) // Postbank || Bank 
	}
	
	@Override
	public Object run() {
		return null;
	}

}

class Opos extends Script {
	
	def CLASSNAME = this.getClass().getName()
	
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
	static final AUSGLAMN       =14 // M: manuell , A: automatisch , N: KonteNnullsaldenlauf
	static def mapAusgl = ["M":"manuell","A":"automatisch","N":"via KonteNnullsaldenlauf"]
	static final BELEGFELD2     =15 // TTMMYY ? Fälligkeit
	static final KZ             =16
	static final BUTEXT         =17
	static final KOST1          =18
	static final KOST2          =19
	static final USTVH          =20 // z.B. 19 7 "div."
	// ...
	private static final ANSRECHPARTNER =43 // last col

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
		return new java.sql.Timestamp(it.getTime())
	}

	// Aufruf : def invoice = opos.makeInvoice(kto,r)
	def makeInvoice = { kto, r ->
		def invoice = [:]
		def soll = range.Item(r, SOLL).Value
		def haben = range.Item(r, HABEN).Value
		invoice.rec = new DecimalFormat("#######").format(rec = range.Item(r, RECHNUNGSNR).Value)
		invoice.amt = soll!=null ? soll : 0 - haben
		invoice.kto = range.Item(r, GEGENKTO).Value
		invoice.dat = toTimestamp(range.Item(r,BUCHDAT).Value)
		invoice.txt = ""
		invoice.ust = range.Item(r,USTVH).Value
		if(SKR03.isDebitor(kto)) { // Ausgangsrechnungen : das Gegenkoto muss ein ErlösKto sein
			if(SKR.isErloesKto(invoice.kto)) {
				// OK
			} else {
				throw new Exception("BP ${kto} Re ${invoice.rec} ist keine Ausgangsrechnung, wg gegenkto ${invoice.kto}")
			}
		} else { // Kreditoren/Lieferanten -> Eingangsrechnungen
			if(invoice.kto==970 || invoice.kto=='div.' || SKR.isAufwendung(invoice.kto) || SKR.isFremdleistung(invoice.kto)) {
				invoice.txt = range.Item(r, BUTEXT).Value
			} else {
				throw new Exception("BP ${kto} Re ${invoice.rec} ist keine Eingangsrechnung, wg gegenkto ${invoice.kto}")
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
		payment.rec = new DecimalFormat("#######").format(rec = range.Item(r, RECHNUNGSNR).Value)
		payment.amt = haben!=null ? haben : 0 - soll
		def gegen = range.Item(r, GEGENKTO).Value
		payment.dat = toTimestamp(range.Item(r,BUCHDAT).Value)
		if(gegen=="div.") {
			payment.kto = gegen
		} else if(SKR.isBank(gegen)) {
			payment.kto = 'Bank'
		} else if(SKR.isSkonto(gegen)) {
			payment.kto = 'Skonto'
		} else {
			throw new Exception("BP ${kto} Re ${payment.rec} ist keine Zahlung, wg gegenkto ${gegen}")
		}
		println "${CLASSNAME}:makePayment ${payment}"
		return payment
	}

	@Override
	public Object run() {
		return null;
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

	def addInvoice = { it ->
		invoiceList.add(it)
	}
	
	def addPayment = { it ->
		paymentList.add(it)
	}
	
	def isDebitor = { it=this.kto.toInteger() ->
		return SKR03.isDebitor(it)
	}
	
	def setDebitor = { it ->
		if(isDebitor(it)) {
			this.kto = new DecimalFormat(SKR03.PERS_KTO_FORMAT).format(it)
		} else {
			throw new Exception("BP ${it} ist kein Debitor")
		}
	}
	
	def isCreditor = { it=this.kto.toInteger() ->
		return SKR03.isCreditor(it)
	}
	
	def setCreditor = { it ->
		if(isCreditor(it)) {
			this.kto = new DecimalFormat(SKR03.PERS_KTO_FORMAT).format(it)
		} else {
			throw new Exception("BP ${it} ist kein Kreditor")
		}
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

}
		
	
	
