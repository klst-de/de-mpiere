package com.klst.importDomain

import groovy.lang.Binding;
import groovy.lang.Script;
import groovy.sql.Sql;

import java.sql.SQLException
import java.util.logging.*
import java.util.logging.Level.KnownLevel

class CleanserScript extends Script {

	def CLASSNAME = "com.klst.importDomain.CleanserScript" //this.getClass().getName()
	Sql sqlInstance;
	public CleanserScript() {
		println "${CLASSNAME}:ctor"
	}

	public CleanserScript(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
		def db = [url:'jdbc:postgresql://localhost/mierp001', user:'adempiere', password:'adempiere', driver:'org.postgresql.Driver']
		try {
			sqlInstance = Sql.newInstance(db.url, db.user, db.password, db.driver)
		} catch (Exception e) {
			println "${CLASSNAME}:ctor ${e} Datenbank ${db.url} nicht erreichbar."
			//throw e
		}
	}

	// returns a list id ids
	def getInactiv = { tablename , keycolumn=[] , retquery=false ->
		if(keycolumn==[]) {
			keycolumn = "${tablename}_id"
		} else if(keycolumn instanceof java.util.List && keycolumn.size()==1) {
			keycolumn = keycolumn[0]
		} else {
			println "${CLASSNAME}:getInactiv PRIMARY KEY = ${keycolumn}."
		}
		
		def sql = """
select ${keycolumn}  
from ${tablename}
where isactive = 'N'
"""
		if(retquery) {
			//println "${CLASSNAME}:getInactiv query = ${sql}."
			return sql
		}
		def inactive = []
		sqlInstance.eachRow(sql,[]) { row ->
			inactive.add(row[0])
		}
		println "${CLASSNAME}:getInactiv found ${inactive.size()} inactive rows in ${tablename}."
		return inactive
	}

	// try to delete
	def tryToDelete = { ids , tablename , keycolumn=[] , ignoreincative=false ->
		Logger log = Logger.getLogger('groovy.sql')
		log.level = Level.SEVERE //.WARNING // damit das intensive LOg von groovy ausschalten
		println "${CLASSNAME}:tryToDelete Logger.Level = ${log.getLevel()}."
		if(keycolumn==[]) {
			keycolumn = "${tablename}_id"
		} else if(keycolumn instanceof java.util.List && keycolumn.size()==1) {
			keycolumn = keycolumn[0]
		} else {
			println "${CLASSNAME}:tryToDelete PRIMARY KEY = ${keycolumn}."
		}
		
		def sql = """
delete from ${tablename}
where isactive = 'N' and ${keycolumn} = ?
"""
		if(ignoreincative) {
			sql = """
delete from ${tablename}
where ${keycolumn} = ?
"""
		}
		def deleted = 0
		def except = 0
		def i = 0
		ids.each { id ->
			try {
				i++
				println "${CLASSNAME}:tryToDelete try to delete id ${id}, deleted=${deleted} ${i}/${ids.size}"
				sqlInstance.execute(sql,[id])
				deleted = deleted + sqlInstance.getUpdateCount()
			} catch (SQLException e) {
				except--
				println "${CLASSNAME}:tryToDelete ${e.getMessage()}"
			} catch (Exception e) {
				println "${CLASSNAME}:tryToDelete ${e}"
			}
		}
		println "${CLASSNAME}:tryToDelete deleted/tries=${deleted}/${ids.size()} in table ${tablename}."
		return deleted
	}
	
	def deleteInactive = { ids , tablename , keycolumn=[] ->
		return tryToDelete(ids, tablename, keycolumn) // damit werden nur die recs gelöscht, die isactive = 'N' haben
	}
	
	// Returns:true if the first result is a ResultSet object; or an update count
	def doSql = { sql , param=[] ->
		def current = sqlInstance.connection.autoCommit = false
		def res
		try {
			def isResultSet = sqlInstance.execute(sql,param)
			if(isResultSet) {
				println "${CLASSNAME}:doSql isQuery : ${sql}"
				res = isResultSet // true
			} else {
				res = sqlInstance.getUpdateCount()
				println "${CLASSNAME}:doSql updates = ${res} : ${sql} param =  ${param}"
				sqlInstance.commit();
			}
		}catch(SQLException ex) {
			println "${CLASSNAME}:doSql ${ex}"
			sqlInstance.rollback()
			println "${CLASSNAME}:doSql Transaction rollback."
		}
		sqlInstance.connection.autoCommit = current
		return res
	}
	

	@Override
	public Object run() {
		println "${CLASSNAME}:run connected to ${sqlInstance.getConnection().metaData.URL}"
		 
		return this;
	}

  // wird in eclipse benötigt, damit ein "Run As Groovy Script" möglich ist (ohne Inhalt)
  // nach dem Instanzieren wird run() ausgeführt
  static main(args) {
  }

}
