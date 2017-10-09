package com.klst.importDomain.product

import com.klst.importDomain.CleanserScript
import groovy.lang.Binding

class ProductCleanser extends CleanserScript {

	def CLASSNAME = this.getClass().getName()
	
	public ProductCleanser() {
		println "${CLASSNAME}:ctor"
	}

	public ProductCleanser(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
	}

	@Override
	public Object run() {
		println "${CLASSNAME}:run"
		println "${CLASSNAME}:run ${this.sqlInstance}"
		return null;
	}

  // wird in eclipse benötigt, damit ein "Run As Groovy Script" möglich ist (ohne Inhalt)
  // nach dem Instanzieren wird run() ausgeführt
  static main(args) {
  }
  
}
