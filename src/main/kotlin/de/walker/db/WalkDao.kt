package de.walker.db

import de.mel.sql.ASQLQueries
import de.mel.sql.Dao
import de.mel.sql.ISQLResource

class WalkDao(sql: ASQLQueries) : Dao(sql) {


    fun insertWalk(walk: Walk): Walk {
        val id = sqlQueries.insert(walk)
        walk.id.v(id)
        return walk
    }

    fun insertFilEntry(entry: WalkerFileEntry): WalkerFileEntry {
        val id = sqlQueries.insert(entry)
        entry.id.v(id)
        return entry
    }

    fun getEntriesResource(walkId: Long): ISQLResource<WalkerFileEntry> {
        return sqlQueries.loadResource(
            WalkerFileEntry().allAttributes,
            WalkerFileEntry::class.java,
            "${WalkerFileEntry.WALKID} = ?",
            ASQLQueries.args(walkId)
        )
    }

    fun update(entry: WalkerFileEntry) {
        sqlQueries.update(entry, "${WalkerFileEntry.ID} = ?", ASQLQueries.args(entry.id.v()))
    }

    fun countEntries(walkId: Long): Long {
        val query = "select count(1) from ${WalkerFileEntry().tableName} where ${WalkerFileEntry.WALKID}=?"
        return sqlQueries.queryValue(query, Long::class.java, ASQLQueries.args(walkId))!!
    }

    fun logHashException(entry: WalkerFileEntry, e: Exception) {
        val exceptionEntry = FileEntryExceptionEntry()
        exceptionEntry.idFileEntry.v(entry.id)
        exceptionEntry.clazz.v(e.javaClass.simpleName)
        exceptionEntry.message.v(e.message)
        val stackTrace = e.stackTrace.take(7)
            .joinToString { "${it.moduleName} - ${it.fileName} - ${it.methodName} - ${it.lineNumber}" }
        exceptionEntry.stacktrace.v(stackTrace)
    }
}