package de.walker.db

import de.mel.sql.Pair
import de.mel.sql.SQLTableObject

class DedupEntry() : SQLTableObject() {

    val id = Pair(Long::class.java, "id")
    val count = Pair(Long::class.java, "c")
    val srcPath = Pair(String::class.java, "srcpath")
    val srcName = Pair(String::class.java, "srcname")
    val srcExt = Pair(String::class.java, "srcext")
    val srcHash = Pair(String::class.java, "srchash")
    val otherPath = Pair(String::class.java, "otherpath")
    val otherName = Pair(String::class.java, "othername")
    val otherExt = Pair(String::class.java, "otherext")
    override fun getTableName() = "NO_TABLE_NAME_HERE"

    init {
        init()
    }

    override fun init() {
        populateInsert(count, srcPath, srcName, srcExt, srcHash, otherPath, otherName, otherExt, id)
        populateAll()
    }
}