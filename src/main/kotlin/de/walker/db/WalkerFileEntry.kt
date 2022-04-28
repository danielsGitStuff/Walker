package de.walker.db

import de.mel.sql.Pair
import de.mel.sql.SQLTableObject

class WalkerFileEntry : SQLTableObject() {
    companion object {
        val ID = "id"
        val WALKID = "walkid"
        val PATH = "path"
        val NAME = "name"
        val EXTENTION = "ext"
        val HASH = "hash"
        val CREATED = "created"
        val MODIFIED = "modified"
    }

    override fun getTableName(): String = "walkfiles"
    val id = Pair(Long::class.java, ID)
    val walkId = Pair(Long::class.java, WALKID)
    val path = Pair(String::class.java, PATH)
    val name = Pair(String::class.java, NAME)
    val extension = Pair(String::class.java, EXTENTION)
    val hash = Pair(String::class.java, HASH)
    val created = Pair(Long::class.java, CREATED)
    val modified = Pair(Long::class.java, MODIFIED)

    init {
        init()
    }

    override fun init() {
        populateInsert(walkId, path, hash, name, extension, created, modified)
        populateAll(id)
    }
}