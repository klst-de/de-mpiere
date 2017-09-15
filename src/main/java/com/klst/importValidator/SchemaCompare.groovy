package com.klst.importValidator

import groovy.lang.Binding;
import groovy.lang.Script;
import groovy.sql.Sql;
import java.sql.SQLException


class SchemaCompare extends Script {

	def CLASSNAME = this.getClass().getName()
	def DEFAULT_FROM_SCHEMA = "mierp001"
	def DEFAULT_TO_SCHEMA = "adempiere"
	Sql sqlInstance;
	
	public SchemaCompare() {
		println "${CLASSNAME}:ctor"
	}

	public SchemaCompare(Binding binding) {
		super(binding);
		println "${CLASSNAME}:ctor binding"
		def db = [url:'jdbc:postgresql://localhost/adempiere_390lts', user:'adempiere', password:'adempiere', driver:'org.postgresql.Driver']
		try {
			sqlInstance = Sql.newInstance(db.url, db.user, db.password, db.driver)
		} catch (Exception e) {
			println "${CLASSNAME}:ctor ${e} Datenbank ${db.url} nicht erreichbar."
			//throw e
		}
	}

	def n_live_tup = [:]
	// returns a map key is table_name , value n_live_tup/aka rows in this table
	def	rowsPerTable = { schema=DEFAULT_FROM_SCHEMA ->
		def sql = """
-- insgesamt tabellen ca #837
select n_live_tup,table_schema,table_name FROM information_schema.tables -- table_schema , table_name
inner join pg_stat_user_tables  on table_name=relname and table_schema = schemaname
WHERE table_schema = :schema
  AND table_type = 'BASE TABLE'
-- - leere
  and n_live_tup>0 -- jetzt nur noch #314
"""
		sqlInstance.eachRow(sql,schema:schema) { row ->
			//println "${row}"
			n_live_tup[row.table_name] = row.n_live_tup
		}
	}
	
	// matches original migration_data.${tablename} and target public.${tablename}
	// returns a list [0,0] if all rows mached
	//                [-1,1] for unmached rows , minus means not imported, plus means data nor in original, each printed out
	def	tablematcher = { tablename , keycolumn=[] ->
		if(keycolumn==[]) {
			keycolumn = "${tablename}_id"
		} else if(keycolumn instanceof java.util.List && keycolumn.size()==1) {
			keycolumn = keycolumn[0]
		} else {
			println "${CLASSNAME}:tablematcher PRIMARY KEY = ${keycolumn}."
		}
		def sql = """
select o.${keycolumn},m.${keycolumn}
,m.* 
from ${DEFAULT_FROM_SCHEMA}.${tablename} o
FULL OUTER JOIN ${tablename} m ON o.${keycolumn} = m.${keycolumn}
"""
		if(keycolumn instanceof java.util.List && keycolumn.size()==2) {
			sql = """
select o.${keycolumn[0]},m.${keycolumn[0]},o.${keycolumn[1]},m.${keycolumn[1]}
,m.* 
from ${DEFAULT_FROM_SCHEMA}.${tablename} o
FULL OUTER JOIN ${tablename} m ON o.${keycolumn[0]} = m.${keycolumn[0]} and o.${keycolumn[1]} = m.${keycolumn[1]}
"""			
		}
		def errs = [0,0]
		//println "${sql}"
		sqlInstance.eachRow(sql,[]) { row ->
			//println "${row}"
			if(row[0]==null) {
				println "+ ${row}"
				errs[1]++
			}
			if(row[1]==null) {
				println "- ${row[0]} fehlt"
				errs[0]--
			}
		}
		return errs
	}
	
	def	match = { tablename , keycolumn=[]->
		def errs = tablematcher(tablename,keycolumn)
		def rets = 0
		if(errs[0]==0 && errs[1]==0) {
			errs = 0
			rets = 0
		} else {
			rets = errs[1] - errs[0]
		}
		def rows = n_live_tup[tablename]  	// rows in original
		println "${CLASSNAME}:run ${errs}/${rows} errors for table ${tablename}."
		return rets
	}

	// returns a map with key=column_name
	def	columns = { tablename , schema=DEFAULT_TO_SCHEMA ->
		//println "${CLASSNAME}:columns for = ${schema}.${tablename}"
		def cols = [:]
		def sql = """
select * 
FROM information_schema.columns 
WHERE table_schema = :schema and table_name = :tablename
"""
		sqlInstance.eachRow(sql,[schema:schema,tablename:tablename]) { row ->
			if(row.getProperties().first) {
				// ...
			}
			//println "${row}"
			cols[row.column_name] = row.ordinal_position
		}
		return cols
	}
	
	def origin = [:]
	def target = [:]
	def commonKeys = null
	def NAMEVALUE = ['name','value']
	def	checkColumns = { full=true , nameValue=[] ->
		def originKeys = origin.keySet()
		def targetKeys = target.keySet()
		commonKeys = originKeys.intersect(targetKeys)
		if(full) {
			return (origin.size()==target.size())
		}
		// gleichnamige spalten:
//		for (int i = 0; i < commonKeys.size(); i++) {
//			println "${commonKeys[i]}: ${origin.get(commonKeys[i])},${target.get(commonKeys[i])}"
//		}
//		println "${CLASSNAME}:checkColumns commonKeys = ${commonKeys.size()} ${commonKeys.intersect(nameValue)} ${nameValue}."
		if(commonKeys.intersect(nameValue).size()>0) {
			return commonKeys
		}
	}

	@Override
	public Object run() {  // nur Test
		println "${CLASSNAME}:run"
		println "${CLASSNAME}:run ${this.sqlInstance}"
		rowsPerTable()
		def tupels = n_live_tup.size()
		println "${CLASSNAME}:run in schema ${DEFAULT_FROM_SCHEMA} gibt es ${tupels} nicht leere Tabellen."
		n_live_tup.sort().each { 
			//println "${it.key}\t\t ${it.value}"
			def tablename = it.key
			def rows = it.value
			//println "${rows}\t ${tablename}"
			origin = columns(tablename,DEFAULT_FROM_SCHEMA)
			target = columns(tablename)
			if(origin.size()==target.size()) {
//				println "${CLASSNAME}:doInsert PASSED number of columns = ${origin.size()}."
			} else {
//				println "${CLASSNAME}:doInsert differnt number of columns: origin = ${origin.size()} <> target = ${target.size()}."
				// uncomment for diff
//				println " ${DEFAULT_FROM_SCHEMA}: ${origin}"
//				println "${DEFAULT_TO_SCHEMA}: ${target}"
				
//				println "${CLASSNAME}:doInsert trying the intersection ..."
			}
			if(checkColumns()!=null) {
//				println "${CLASSNAME}:doInsert PASSED number of intersect columns = ${commonKeys.size()}."
			} else {
				throw new RuntimeException("keine matching Spalten gefunden")
			}
			def minus = origin.size() - commonKeys.size()  // Spalten die verloren gehen
			def plus  = target.size() - commonKeys.size()
			println "\t-${minus} \t${commonKeys.size()} \t+${plus} \t${it.key} \t#=\t${it.value}"
		}
		//n_live_tup.each { doTest(it.key) }
		return this;
	}

  // wird in eclipse benötigt, damit ein "Run As Groovy Script" möglich ist (ohne Inhalt)
  // nach dem Instanzieren wird run() ausgeführt
  static main(args) {
  }

}
