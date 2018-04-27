// Feat #1405, #1687
package com.klst.mierp.itdz

import groovy.lang.Binding
import groovy.lang.Script
import java.text.DecimalFormat
import java.text.ParsePosition
import java.text.SimpleDateFormat
import java.util.Properties

import org.codehaus.groovy.scriptom.ActiveXObject
import com.jacob.com.ComFailException
import org.compiere.model.MBPartner
import org.compiere.model.X_C_Order
import org.compiere.model.MOrder
import org.compiere.model.MOrderLine
import org.compiere.model.MPInstance
import org.compiere.model.MPInstancePara
import org.compiere.model.MProduct
import org.compiere.model.MUser
import org.compiere.util.DB


class CreateITDZOrders extends Script {
	
	def CLASSNAME = this.getClass().getName()

	public CreateITDZOrders() {
		println "${CLASSNAME}:ctor"
	}

	public CreateITDZOrders(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}
	
	def msg = new StringBuilder()
	def addMsg = { it , m=this.msg ->
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
				if(ctx) {
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
			this._pi = new MPInstance(this._Ctx, A_AD_PInstance_ID, null)
			println "${CLASSNAME}:isProcess ${this._pi}"
			MPInstancePara[] paraList = this._pi.getParameters()
			for (para in paraList) {				
				println "${CLASSNAME}:isProcess ${para.getParameterName()} ${para.getP_String()}"
				if(para.getParameterName()=="FileName") {
					this.pXls = para.getP_String()
				}
			}
		}
		return ret
	}

	def envGetContext = { it , WindowNo=this.getProperty("A_WindowNo") -> //.toInteger() ->
		String context = "${WindowNo}|${it}"
		println "${CLASSNAME}:envGetContext result:"+ctx().get(context)
		return ctx().get(context)
	}


	private static final ITDZ_ID = 1000296
	private static final ITDZ_LOCATION_ID = 1000294 // c_bpartner_location_id
	private static final ESELLING_ID = 1000452 // ad_user
	
	def makeOrder = { poreference, description, dateOrdered, datePromised, bp, cu, String trxName=this._TrxName, ctx=this._Ctx ->		
		def sql = """
SELECT * FROM c_order
WHERE ad_client_id = ${this._pi.getAD_Client_ID()} AND ad_org_id IN( 0 , ${this._pi.getAD_Org_ID()} ) AND isactive = 'Y'
  AND poreference = '${poreference}' AND issotrx = 'Y'
"""
		println "${CLASSNAME}:makeOrder ${sql}"
		def pstmt = DB.prepareStatement(sql, trxName)
		def resultSet = pstmt.executeQuery()
		MOrder mOrder = null
		if(resultSet) {
			while(resultSet.next()) {
				mOrder = new MOrder(ctx, resultSet, trxName)
				println "${CLASSNAME}:makeOrder exist ${mOrder}"
			}
		}
		if(mOrder) {
			addMsg("Order ${description} existiert bereits: ${mOrder}")
			return [mOrder,false] 
		} else {
			mOrder = new MOrder(ctx, 0, trxName)
			mOrder.setC_DocTypeTarget_ID(1000030) // == Standard Order
			mOrder.setPOReference(poreference)
			mOrder.setDescription(description)
			mOrder.setDateOrdered(dateOrdered)
			mOrder.setDatePromised(datePromised)
			mOrder.setBPartner(bp)
			mOrder.setAD_User_ID(cu.getAD_User_ID()) // contact user
			mOrder.setBill_BPartner_ID(ITDZ_ID)
			mOrder.setBill_Location_ID(ITDZ_LOCATION_ID)
			mOrder.setBill_User_ID(ESELLING_ID) // Rechnung an ITDZ.eSelling
			mOrder.setPaymentRule(X_C_Order.PAYMENTRULE_OnCredit) // Rechnungsstellung
			mOrder.setC_PaymentTerm_ID( mOrder.getBill_BPartner().getC_PaymentTerm_ID() ) // Zahlungsbedingung
			mOrder.setDeliveryViaRule(X_C_Order.DELIVERYVIARULE_Shipper)
			mOrder.setM_Shipper_ID(1000000) // == Standard - Frei Haus
			mOrder.setInvoiceRule(X_C_Order.INVOICERULE_AfterDelivery)
			mOrder.setC_OrderSource_ID(1000001);  // == "ITDZorder"
			mOrder.saveEx()	
		}
		addMsg("Order ${description} erstellt: DocumentNo=${mOrder.getDocumentNo()}")
		return [mOrder,true]
	}
	
	def addOrderLine = { order, pos, productValue, qty, netto, String trxName=this._TrxName, ctx=this._Ctx ->
		def lineno = getDoubleOrStringToString(pos)
		def pv = getDoubleOrStringToString(productValue)
		def sql = """
SELECT * FROM m_product
WHERE ad_client_id = ${this._pi.getAD_Client_ID()} AND ad_org_id IN( 0 , ${this._pi.getAD_Org_ID()} ) AND isactive = 'Y'
  AND value = '${pv}-ITDZ'
"""
		println "${CLASSNAME}:addOrderLine ${sql}"
		def pstmt = DB.prepareStatement(sql, trxName)
		def resultSet = pstmt.executeQuery()
		MProduct obj = null
		def saved = false
		if(resultSet) {
			while(resultSet.next()) { // muss nicht eindeutig sein!
				obj = new MProduct(ctx, resultSet, trxName)
				println "${CLASSNAME}:addOrderLine ${obj}"
			}
		} 
		if(obj==null) {
			addMsg("* Position ${lineno} '${pv}' Produkt nicht gefunden")
			return saved
		}
		MOrderLine ol = new MOrderLine(ctx, 0, trxName)
		ol.setLine(Integer.parseInt(lineno))
		ol.setM_Product_ID(obj.getM_Product_ID())
		ol.setC_Order_ID(order.getC_Order_ID())
		ol.setQty(qty)
		if(ol.beforeSave(true)) {
			saved = true
			ol.saveEx()
		}
		BigDecimal bigDecimalnetto = new BigDecimal(netto);
		if(bigDecimalnetto.subtract(ol.getLineNetAmt()).abs()<0.01) {
			//println "${CLASSNAME}:addOrderLine OK ${ol}"
		} else {
			println "${CLASSNAME}:addOrderLine NOK ${ol} ${netto} <> ${ol.getLineNetAmt()} ${bigDecimalnetto.subtract(ol.getLineNetAmt())}"
			addMsg("* Position ${lineno} '${pv}' überprüfen")
			ol.setDescription("Position überprüfen")
			ol.saveEx()
			saved = false
		}
		return saved
	}
		
	def getBPartner = { likename, String trxName=this._TrxName, ctx=this._Ctx ->
// 'Hans-und-Hilde-Coppi-Schule (11Y05)' not found wg (11Y05) "Hans und Hilde Coppi-Gymnasium"
// 'Lew-Tolstoi Grundschule' nicht gefunden wg "Lew Tolstoi Grundschule" in DB, daher upper-Suche mit like:
		String uname = likename.toLowerCase().replaceAll("[^\\x61-\\x7A]", "%").toUpperCase()
		// auch c_bp_relation.c_bpartner_location_id
		def sql = """
SELECT * FROM c_bpartner
WHERE ad_client_id = ${this._pi.getAD_Client_ID()} AND ad_org_id IN( 0 , ${this._pi.getAD_Org_ID()} ) AND isactive = 'Y'
  AND upper(name) like '%${uname}%'
  AND c_bpartner_id IN( SELECT c_bpartner_id FROM c_bp_relation 
    WHERE ad_client_id = ${this._pi.getAD_Client_ID()} AND ad_org_id IN( 0 , ${this._pi.getAD_Org_ID()} ) AND isactive = 'Y'
      AND c_bpartnerrelation_id = ${ITDZ_ID} AND isbillto = 'Y' )
"""
		println "${CLASSNAME}:getBPartner ${sql}"
		def pstmt = DB.prepareStatement(sql, trxName)
		def resultSet = pstmt.executeQuery()
		def bpList = []	// empty
		MBPartner obj = null
		if(resultSet) {
			while(resultSet.next()) { // muss nicht eindeutig sein!
				obj = new MBPartner(ctx, resultSet, trxName)
				println "${CLASSNAME}:getBPartner ${obj}"
				bpList.add(obj)
			}
		}  
		if(bpList.isEmpty()) {
			println "${CLASSNAME}:getBPartner keinen Partner gefunden für '${likename}'"
			addMsg("keinen Partner zum Liefern gefunden für '${likename}' - ersatzweise ITDZ")
			// nehme ITDZ
			obj = new MBPartner(ctx, ITDZ_ID, trxName)
			bpList.add(obj)
		} else if(bpList.size()>1) {
			println "${CLASSNAME}:getBPartner mehrere Partner gefunden für '${likename}'"
			// Eindeutigkeit
		}
		return bpList.get(0) // first!
	}
	
	def getContactUser = { bp, telNr, String trxName=this._TrxName, ctx=this._Ctx ->
		String telNo = telNr.replaceAll("[^\\x30-\\x39]", "")
		def sql = """
SELECT * FROM ad_user
WHERE ad_client_id = ${this._pi.getAD_Client_ID()} AND ad_org_id IN( 0 , ${this._pi.getAD_Org_ID()} ) AND isactive = 'Y'
  AND c_bpartner_id = ${bp.getC_BPartner_ID()}
"""
		println "${CLASSNAME}:getContactUser ${sql}"
		def pstmt = DB.prepareStatement(sql, trxName)
		def resultSet = pstmt.executeQuery()
		def urList = []	// empty
		def zentrale = []
		MUser obj = null
		if(resultSet) {
			while(resultSet.next()) { // muss nicht eindeutig sein!
				obj = new MUser(ctx, resultSet, trxName)
				def phone = obj.getPhone()
				println "${CLASSNAME}:getContactUser ${obj} phone=${phone}"
				if(phone==null) {
					phone = 'null'
				} else {
					phone = phone.replaceAll("[^\\x30-\\x39]", "")
				}
				if(telNo.endsWith(phone)) {
					urList.add(obj)
				} else {
					println "${CLASSNAME}:getContactUser ${obj} raus, da ${phone} <> ${telNr}"
				}
				if(obj.getName()=="Zentrale" || obj.getName()=="eSelling,") {
					zentrale.add(obj)
				}
			}
		}
		if(urList.intersect(zentrale).size()<=1) {
			if(zentrale.size==0) {
				addMsg("no contactUser with phone '${telNr}' - ersatzweise ITDZ")
				return new MUser(ctx, ESELLING_ID, trxName)
			}
			obj = zentrale.get(0)
			return obj 
		} else {
			obj = urList.get(0)
		}
		println "${CLASSNAME}:getContactUser result: ${obj}"
		return obj
	}
		
	private static final ACTIVEX_FSO = "Scripting.FileSystemObject"
	private static final ACTIVEX_EXCEL = "Excel.Application"
	def getExcel = { ->
		def objExcel = new ActiveXObject(ACTIVEX_EXCEL)
		println "${CLASSNAME}:getExcel Version ${objExcel.Version}"
		return objExcel
	}
	
	private static final XLSX_EXT = ".xlsx"
	private static final XLSDIR_TEST = "C:\\proj\\minhoff\\ITDZ-Bestellung\\"
	def getSheet = { excel, file=this.pXls ->
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

	def toTimestamp = { it, format="EEE MMM dd hh:mm:ss zzz yyyy" ->
//		SimpleDateFormat dateFormat = new SimpleDateFormat(format)
//		Date parsedDate = dateFormat.parse(it, new ParsePosition(0))
//		return new java.sql.Timestamp(parsedDate.getTime())
		if(it==null) {
			return null
		}
		return new java.sql.Timestamp(it.getTime())
	}

	// to get BELEGNUMMER or MATERTAL ...
	def getDoubleOrStringToString = { it
		if(it==null) {
			return null
		}
		if(it instanceof Double) {
			return new DecimalFormat("#############").format(it)
		} else {
			return it.trim()
		}
	}
	
	// columns
	private static final BELEGNUMMER    = 1
	private static final VERTR_BEL      = 2
	private static final VB_POS         = 3
	private static final EKG            = 4
	private static final LIEFERANT      = 5
	private static final LIEFERANT_NAME = 6
	private static final BELEGDAT       = 7 // java.util.Date
	private static final POS            = 8
	private static final MATERTAL       = 9
	private static final POSTEXT        =10
	private static final NETTO          =11
	private static final BRUTTO         =12
	private static final MENGE          =13
	private static final KUNDENBESTNO   =14 // Kundenbestellnummer
	private static final LIEFDAT        =15
	private static final LIEFMATNO      =16
	private static final LIEFERADR1     =17
	private static final LIEFERADRTEL   =25
	private static final LIEFERADRFAX   =26
	def getValue = { range, row , col->
		return range.Item(row,col).Value 
	}
	def getOrders = { sheet , orderList ->
		println "${CLASSNAME}:getOrders UsedRange Rows=${sheet.UsedRange.Rows.Count} Columns=${sheet.UsedRange.Columns.Count}"
		if(sheet.UsedRange.Rows.Count>1 && sheet.UsedRange.Columns.Count==26) {
			// OK
		} else {
			println "${CLASSNAME}:getOrders UsedRange check faild: Rows=${sheet.UsedRange.Rows.Count}/expected>1 Columns=${sheet.UsedRange.Columns.Count}/expected=26"
			return
		}
		def range = sheet.UsedRange
		def lastValue = range.Item(1,BELEGNUMMER).Value
		println "${CLASSNAME}:getOrders Belegnummer=${range.Item(1,BELEGNUMMER).Value}"
		def makeResult = []
		def posCnt = 0
		for( r=2; r<=range.Rows.Count; r++) {
			String belegnummer = null
			def belegStringOrDouble = range.Item(r, BELEGNUMMER).Value
			println "${CLASSNAME}:getOrders r=${r} belegStringOrDouble=${belegStringOrDouble}"
			belegnummer = getDoubleOrStringToString(belegStringOrDouble)
			if(lastValue==belegnummer) {
				// same order
			} else if(belegnummer==null || belegnummer=="") {
				// das sollte nicht passieren, aber im ITDZ-Beispiel ist am Ende eine Summenzeile
				return
			} else {
				if(!makeResult.isEmpty() && makeResult.get(1)) {
					addMsg("Order mit ${posCnt} Positionen fertiggestellt, weitere Order vorhanden ...")
					makeResult = []
					posCnt = 0
				}
				def belegdat= toTimestamp(range.Item(r,BELEGDAT).Value)
				println "${CLASSNAME}:getOrders Belegnummer=${range.Item(r,BELEGNUMMER).Value} Lieferdat=${range.Item(r,LIEFERADR1).Value} ${belegdat}"
				def liefdat= toTimestamp(range.Item(r,LIEFDAT).Value)
				def adr1 = range.Item(r,LIEFERADR1).Value
				MBPartner bp = getBPartner(adr1)
				MUser contactUser = getContactUser(bp, range.Item(r,LIEFERADRTEL).Value)
				def desc = range.Item(r,EKG).Value+'-'+belegnummer+'-'+range.Item(r,VERTR_BEL).Value // zu lang >20 für poreference
				makeResult = makeOrder(belegnummer,desc,belegdat,liefdat,bp,contactUser)
				orderList.add(makeResult.get(0)) // [order,isNew]
			}
			if(makeResult.get(1)) {
				def added = addOrderLine(makeResult.get(0), range.Item(r,POS).Value, range.Item(r,MATERTAL).Value, range.Item(r,MENGE).Value, range.Item(r,NETTO).Value)
				if(added) {
					posCnt++
				}
			} else {
				println "${CLASSNAME}:getOrders ${range.Item(r,POS).Value} ${getValue(range,r,MATERTAL)} ${makeResult.get(0)}"
			}
			lastValue = belegnummer
		}//for
		return orderList
	}
		
	@Override
	public Object run() {
		println "${CLASSNAME}:run"
		def excel = getExcel()
		def sheet = null
		if(isProcess()) try {
			sheet = getSheet(excel)
			addMsg("excel gelesen - ${sheet.UsedRange.Rows.Count} Zeilen")
		    def orderList = []	// empty
			getOrders(sheet,orderList)
			addMsg("${orderList.size} Order.")
		} catch(ComFailException e) {
			excel.Workbooks.Close()
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
//		Script script = new CreateITDZOrders()
//		script.run()
	}
  
}
