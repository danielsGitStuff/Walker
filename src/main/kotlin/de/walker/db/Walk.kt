package de.walker.db

import de.mel.sql.Pair
import de.mel.sql.SQLTableObject

class Walk() : SQLTableObject() {
    companion object{
        val ID = "id"
        val DIR = "dir"
    }
    override fun getTableName(): String = "walk"
    val id = Pair(Long::class.java, ID)
    val dir = Pair(String::class.java, DIR)

    init {
        init()
    }

    override fun init() {
        populateInsert(dir)
        populateAll(id)
    }
}