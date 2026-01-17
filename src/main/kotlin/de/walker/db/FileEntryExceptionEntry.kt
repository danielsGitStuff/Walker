package de.walker.db

import de.mel.sql.Pair
import de.mel.sql.SQLTableObject

class FileEntryExceptionEntry : SQLTableObject() {

    companion object {
        val ID = "id"
        val ID_FILE_ENTRY = "id_file_entry"
        val CLAZZ = "clazz"
        val MESSAGE = "message"
        val STACKTRACE = "stacktrace"
    }

    init {
        init()
    }

    override fun getTableName() = "file_exceptions"

    val id = Pair(Long.Companion::class.java, ID)
    val idFileEntry = Pair(Long::class.java, ID_FILE_ENTRY)
    val clazz = Pair(String::class.java, CLAZZ)
    val message = Pair(String::class.java, MESSAGE)
    val stacktrace = Pair(String::class.java, STACKTRACE)

    override fun init() {
        populateInsert(idFileEntry, clazz, message, stacktrace)
        populateAll(id)
    }
}