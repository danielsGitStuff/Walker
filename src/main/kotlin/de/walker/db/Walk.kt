package de.walker.db

import de.mel.sql.Pair
import de.mel.sql.SQLTableObject

class Walk() : SQLTableObject() {
    override fun getTableName(): String = "walk"
    val id = Pair(Long::class.java, "id")
    val dir = Pair(String::class.java, "dir")
    val identifier = Pair(String::class.java, "identifier")

    init {
        init()
    }

    override fun init() {
        populateInsert(dir, identifier)
        populateAll(id)
    }
}