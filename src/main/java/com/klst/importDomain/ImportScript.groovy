package com.klst.importDomain

import groovy.lang.Binding;
import groovy.lang.Script;
import groovy.sql.Sql;
import java.sql.SQLException


class ImportScript extends Script {

	def CLASSNAME = "com.klst.importDomain.ImportScript" //this.getClass().getName()
	def DEFAULT_FROM_SCHEMA = "mierp001"
	def DEFAULT_TO_SCHEMA = "adempiere"
	def SUPER_USER_ID = 100
	def SYSTEM_CLIENT_ID = 0
	def GERDENWORD_CLIENT_ID = 11
	def DEFAULT_CLIENT_ID = 1000000
	Sql sqlInstance;
	public ValidatorScript() {
		println "${CLASSNAME}:ctor"
	}

	public ImportScript(Binding binding) {
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
	
	def	makeDelete = { tablename ->
		def sql = """
DELETE FROM ${tablename} 
     WHERE ad_client_id=?
"""
		return sql			
	}
	
	def	makeInsert = { tablename , nullcolumns=[] , useSuperUser=false , SuperUserId=SUPER_USER_ID->
		def selo = (commonKeys.collect  { "$it" } as Iterable).join(', ')
		def selm = (commonKeys.collect  { "m.$it" } as Iterable).join(', ')
		if(useSuperUser) { // use SuperUserId for cols
			selm = selm.replaceFirst("m.createdby", SuperUserId.toString()).replaceFirst("m.updatedby", SuperUserId.toString())
		}
		println "${CLASSNAME}:makeInsert size ${nullcolumns.size()}"
		nullcolumns.each { selm = selm.replaceFirst("m.${it}", "null") }
		println "${CLASSNAME}:makeInsert ${selm}"
		def sql = """
INSERT INTO ${tablename} ( ${selo} ) 
    select ${selm} from ${DEFAULT_FROM_SCHEMA}.${tablename} AS m
     WHERE m.ad_client_id=?
"""
		return sql			
	}
	
	def doInsert = { tablename , nullcolumns=[] , clientID=DEFAULT_CLIENT_ID , useSuperUser=false ->
		println "${CLASSNAME}:doInsert ${tablename} nullcolumns = ${nullcolumns}."
		if(n_live_tup.get(tablename)==null) {
			println "${CLASSNAME}:doInsert ${tablename} leer -> nix zu tun."
			return 0
		}
		origin = columns(tablename,DEFAULT_FROM_SCHEMA)
		target = columns(tablename)
		if(origin.size()==target.size()) {
			println "${CLASSNAME}:doInsert PASSED number of columns = ${origin.size()}."
		} else {
			println "${CLASSNAME}:doInsert differnt number of columns: origin = ${origin.size()} <> target = ${target.size()}."
			println "${origin.size()}: ${origin}"
			println "${target.size()}: ${target}"
			println "${CLASSNAME}:doInsert trying the intersection ..."
		}
		if(checkColumns()!=null) {
			println "${CLASSNAME}:doInsert PASSED number of intersect columns = ${commonKeys.size()}."
		} else {
			throw new RuntimeException("keine matching Spalten gefunden")
		}
		
		//return makeInsert(tablename,nullcolumns) //test
		// vorher löschen
		def sql = makeDelete(tablename)
		println "${sql}"

		def deletes = 0
		def inserts = 0
		sqlInstance.connection.autoCommit = false
		try {
			sqlInstance.execute(sql,[clientID])  
			deletes = sqlInstance.getUpdateCount()
			println "${CLASSNAME}:doInsert deletes = ${deletes}."
				
			sql = makeInsert(tablename,nullcolumns,useSuperUser)
			println "${sql}"
			sqlInstance.execute(sql,[clientID])  
			inserts = sqlInstance.getUpdateCount()
			println "${CLASSNAME}:doInsert inserts = ${inserts}."
			
			sqlInstance.commit();
		}catch(SQLException ex) {
			println "${CLASSNAME}:doInsert ${ex}"
			sqlInstance.rollback()
			println "${CLASSNAME}:doInsert Transaction rollback." 
		}

		return inserts-deletes
	}
	
	def	makeMerge = { tablename , keycolumns=[] ->
		def selo = (commonKeys.collect  { "$it" } as Iterable).join(', ')
		def selm = (commonKeys.collect  { "m.$it" } as Iterable).join(', ')
		def updc = (commonKeys.collect  { "$it = m.$it" } as Iterable).join(', ')
		def sql = """
INSERT INTO ${tablename} ( ${selo} ) 
    select ${selm} from ${DEFAULT_FROM_SCHEMA}.${tablename} AS m
ON CONFLICT (${keycolumns[0]}) 
    DO UPDATE SET ( ${selo} ) 
= ( select ${selm} from ${DEFAULT_FROM_SCHEMA}.${tablename} AS m , ${tablename} AS o 
    where o.${keycolumns[0]} = m.${keycolumns[0]} and m.ad_client_id=? ) 
   WHERE ${tablename}.ad_client_id=?
"""
		return sql			
	}
		
	def doMerge = { tablename , keycolumns=[] , nameValue=NAMEVALUE ->
		println "${CLASSNAME}:doMerge ${tablename} PRIMARY KEY = ${keycolumns}."
		if(n_live_tup.get(tablename)==null) {
			println "${CLASSNAME}:doMerge ${tablename} leer -> nix zu tun."
			return 0
		}
		origin = columns(tablename,DEFAULT_FROM_SCHEMA)
		target = columns(tablename)
		if(origin.size()==target.size()) {
			println "${CLASSNAME}:doMerge PASSED number of columns = ${origin.size()}."
		} else {
			println "${CLASSNAME}:doMerge differnt number of columns: origin = ${origin.size()} <> target = ${target.size()}."
			println "${origin.size()}: ${origin}"
			println "${target.size()}: ${target}"
			println "${CLASSNAME}:doMerge trying the intersection ..."
		}
		if(checkColumns(false,nameValue)!=null) {
			println "${CLASSNAME}:doMerge PASSED number of intersect columns = ${commonKeys.size()}."
		} else {
			throw new RuntimeException("keine matching Spalten gefunden")
		}
		def sql = makeMerge(tablename , keycolumns)
		println "${sql}"
		
		def updates = 0
		sqlInstance.connection.autoCommit = false
		try {
			sqlInstance.execute(sql,[DEFAULT_CLIENT_ID,DEFAULT_CLIENT_ID])
			updates = sqlInstance.getUpdateCount()
			println "${CLASSNAME}:doMerge updates = ${updates}."
			sqlInstance.commit();
		}catch(SQLException ex) {
			println "${CLASSNAME}:doMerge ${ex}"
			sqlInstance.rollback()
			println "${CLASSNAME}:doMerge Transaction rollback." 
		}
		return updates
	}
	
	@Override
	public Object run() {  // nur Test
		println "${CLASSNAME}:run"
		println "${CLASSNAME}:run ${this.sqlInstance}"
		rowsPerTable()
		def tupels = n_live_tup.size()
		println "${CLASSNAME}:run tupels=${tupels}"
		assert(0==doMerge("i_asset"))
		//assert(0==doMerge("ad_org",['ad_org_id']))
		//assert(0==doInsert("c_region",['c_region_id']))
		def sql = doInsert("ad_user",["c_bpartner_id","c_bpartner_location_id"])
		println "${CLASSNAME}:run doInsert ad_user \n${sql}"
		sql = doInsert("c_greeting") 
		println "${CLASSNAME}:run doInsert ad_user \n${sql}"
		return this;
	}

  // wird in eclipse benötigt, damit ein "Run As Groovy Script" möglich ist (ohne Inhalt)
  // nach dem Instanzieren wird run() ausgeführt
  static main(args) {
  }

}
