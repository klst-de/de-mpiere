// Feature #1747 : 22.06.2018 - bridge to com.klst.mierp.process.OrderPOCreate
package com.klst.mierp.process

import groovy.lang.Binding
import groovy.lang.Script

import com.klst.mierp.process.OrderPOCreate
import org.compiere.process.ProcessInfo
import org.compiere.process.ProcessInfoParameter

class OrderManufacturingAndPOCreate extends Script {

	def CLASSNAME = this.getClass().getName()

	public OrderManufacturingAndPOCreate() {
		println "${CLASSNAME}:ctor"
	}

	public OrderManufacturingAndPOCreate(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}

	// dynamisch geladene AD Klassen
	Class MPInstance = null
	Class MOrder = null
//	Class OrderPOCreate = null

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
			}
			MOrder = this.class.classLoader.loadClass("org.compiere.model.MOrder", true, false )
//			OrderPOCreate = this.class.classLoader.loadClass("com.klst.mierp.process.OrderPOCreate", true, false )
		}
		return ret
	}

//	def getPara = { it ->
//		for (para in paraList) {
//			if(para.getParameterName()==it) {
//				println "${CLASSNAME}:getPara ${it} ${para.getP_String()},  getP_Number=${para.getP_Number()} ,  getP_Number=${para.getInfo()}"
//				//return para.getP_String()
//				return para.getP_Number()  // liefert BigDecimal
//			}
//		}
////		println "${CLASSNAME}:getPara '${it}' nicht gefunden"
//	}
//
//	def getOrder = { c_order_id, trxName=this._TrxName, ctx=this._Ctx ->
//		def mOrder = MOrder.newInstance(ctx, c_order_id, trxName)
//		return mOrder
//	}
	
	def makeProcessInfo = { processInfoTitle="OrderPOCreate" ->
		def processId = this._pi.getAD_Process_ID()
		println "${CLASSNAME}:makeProcessInfo processId=${processId}"
		ProcessInfo pInfo = new ProcessInfo(processInfoTitle, processId)
		pInfo.setAD_PInstance_ID(A_AD_PInstance_ID)
		for (para in paraList) {
			println "${CLASSNAME}:makeProcessInfo para ${para.getParameterName()}: getP_String=${para.getP_String()}, getP_Number=${para.getP_Number()}, getInfo=${para.getInfo()}"
			if(para.getParameterName()=="C_Order_ID") {
				pInfo.addParameter(para.getParameterName(), new Integer(para.getP_Number().intValue()), para.getInfo())
			}
			if(para.getParameterName()=="ProcessAnyStatus") {
				pInfo.addParameter(para.getParameterName(), para.getP_String(), para.getInfo())
			}
		}
		return pInfo
	} 
	
	def startProcess = { pInfo, trx=A_Trx, ctx=this._Ctx ->
		OrderPOCreate orderPOCreate = new OrderPOCreate()
		/* die rechts markierten Lieferanten werden von Bestellungen ausgenommen:
		 36518; 1;"70375";"70375";"Ingram Micro Distribution GmbH"
		 36863; 1;"70813";"70813";"MEDIUM GmbH"
		 39102; 1;"83007";"83007";"Tech Data GmbH & Co. oHG"
		 37111; 1;"71067";"71067";"OBETA ELEKTRO SB"               nein
		 38536; 1;"72504";"72504";"Optoma Deutschland GmbH"        nein
		1000052;2;"73024";"73024";"Remonta GmbH"                   nein
		 38645; 3;"72613";"72613";"Kramer Germany GmbH"            nein
		 39032; 4;"73005";"73005";"digitalequipment"               nein
		 38867; 5;"72835";"72835";"Nedis GmbH"                       - Kabel
		 39023; 6;"72996";"72996";"PureLink GmbH"                    - Kabel
		 38976; 7;"72947";"72947";"BÜROPARTNER - Mediasprint"        - Pylone
		 37008;12;"70962";"70962";"Fröhlich & Walter GmbH"           - Kabel
		 38624;12;"72592";"72592";"Kern & Stelly"
		 38333;31;"72300";"72300";"Info Tech 24 GmbH"
		 */
		// TODO besser: Produkte sind auf Lager mit einem Mindestbestand : M_Replenish
		orderPOCreate.excludedVendor(37111);  
		orderPOCreate.excludedVendor(38536);  
		orderPOCreate.excludedVendor(1000052);  
		orderPOCreate.excludedVendor(38645);  
		orderPOCreate.excludedVendor(39032);  
		orderPOCreate.excludedVendor(38867); // Kabel
		orderPOCreate.excludedVendor(39023); // Kabel
		orderPOCreate.excludedVendor(37008); // Kabel 
		orderPOCreate.excludedVendor(38976); // Pylone etc 
		def b = false
		try {
			b = orderPOCreate.startProcess(ctx, pInfo, trx)  // aus SvrProcess
			// @return true if the next process should be performed
			def summary = pInfo.getSummary() // public String getSummary()
			println "${CLASSNAME}:startProcess ${b} : true if the next process should be performed, summary=${summary}"
			if(b) {
				addMsg(summary)
			} else {
				addMsg(summary) // TODO wie bekomme ich es rot?
			}
		} catch(Exception e) {
			println "${CLASSNAME}:startProcess ${e.getMessage()}"
		}
		return b
	}
	
	@Override
	public Object run() {
		println "${CLASSNAME}:run"
		if(isProcess()) {
			println "${CLASSNAME}:run isProcess"
//			def C_Order_ID=getPara("C_Order_ID").intValue()
//			def mOrder = getOrder(C_Order_ID)
//			println "${CLASSNAME}:run mOrder: ${mOrder}"
			
			ProcessInfo pInfo = makeProcessInfo()
			println "${CLASSNAME}:run pInfo.getAD_PInstance_ID=${pInfo.getAD_PInstance_ID()}"
			
			def b = startProcess(pInfo)
			
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
