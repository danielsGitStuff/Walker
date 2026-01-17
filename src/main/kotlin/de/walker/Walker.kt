package de.walker

import de.mel.core.serialize.serialize.tools.OTimer
import de.mel.execute.SqliteExecutor
import de.mel.sql.*
import de.mel.sql.conn.SQLConnector
import de.mel.sql.transform.SqlResultTransformer
import de.walker.db.Walk
import de.walker.db.WalkDao
import de.walker.db.WalkerFileEntry
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import kotlin.io.path.fileSize

class Walker(val config: Config) {
    var connection = SQLConnector.createSqliteConnection(File(config.dbFile))
    val sql: SQLQueries = SQLQueries(connection, SqlResultTransformer.sqliteResultSetTransformer())
    val dao = WalkDao(sql)
    fun start() {
        sql.execute("PRAGMA foreign_keys = ON")
        sql.enableWAL()
        val ex = SqliteExecutor(connection)
        if (listOf("walk", "walkfiles").any { !ex.checkTableExists(it) }) {
            val ins = WalkDao.createSql().byteInputStream()
            ex.executeStream(ins)
            println("db initialised")
        }
        val rootDir = File(config.dir1)
        val walk = Walk()
        walk.dir.v(rootDir.absolutePath)
        dao.insertWalk(walk)
        val walkId = walk.id.v()

        val whiteList = setOf("jpg", "jpeg", "png", "bmp", "gif", "dng", "raw", "mp4", "psd", "webp")
        val t1 = OTimer("relativeTo")
        val t2 = OTimer("insert")

        this.sql.beginTransaction()
        rootDir.walkTopDown().filter { it.isFile }.forEach { file ->
            if ((!config.whiteList || file.extension.lowercase() in whiteList) && !Files.isSymbolicLink(file.toPath())) {
                val f = file.relativeTo(rootDir)
                try {
                    val entry = WalkerFileEntry()
                    entry.hash.v(if (config.saveHash) file.inputStream().use { Hash.md5(it) } else null)
                    entry.walkId.v(walkId)
                    entry.path.v(f.parentFile?.path)
                    entry.extension.v(f.extension)
                    entry.name.v(f.nameWithoutExtension.lowercase())
                    entry.size.v(file.toPath().fileSize())
                    entry.modified.v(file.lastModified() / 1000)
                    val o = Files.readAttributes(file.toPath(), BasicFileAttributes::class.java)
                    entry.created.v(o.creationTime().toMillis() / 1000)
                    // to query in SQLite
                    // select strftime("%H:%M %m-%d-%Y", created, 'unixepoch') from walkfiles
                    t2.start()
                    dao.insertFilEntry(entry)
                    t2.stop()
                } catch (e: Exception) {
                    println("could not process '${file.path}}' because '${e}'")
                }
            }
        }
        this.sql.commit()
        t1.print()
        t2.print()
    }

}

