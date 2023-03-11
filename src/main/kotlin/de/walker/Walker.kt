package de.walker

import de.mel.Lok
import de.mel.core.serialize.serialize.tools.OTimer
import de.mel.execute.SqliteExecutor
import de.mel.sql.Hash
import de.mel.sql.SQLQueries
import de.mel.sql.conn.SQLConnector
import de.mel.sql.transform.SqlResultTransformer
import de.walker.db.Walk
import de.walker.db.WalkDao
import de.walker.db.WalkerFileEntry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.File
import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes

class Walker(val config: Config) {
    var connection = SQLConnector.createSqliteConnection(File(config.dbFile))
    val sql: SQLQueries = SQLQueries(connection, SqlResultTransformer.sqliteResultSetTransformer())
    val dao = WalkDao(sql)

    fun start() {
        when (config.mode) {
            Mode.INDEX -> index()
            Mode.DEDUPLICATE -> deduplicate()
        }
    }
    fun deduplicate(){
        val duplicates = dao.findDebug(config.walkIdentifier)
        Lok.debug("asdasd")
    }

    fun index() {
        sql.execute("PRAGMA foreign_keys = ON")
//        sql.enableWAL()
        val ex = SqliteExecutor(connection)
        if (listOf("walk", "walkfiles").any { !ex.checkTableExists(it) }) {
            val ins = WalkDao.createSql().byteInputStream()
            ex.executeStream(ins)
            println("db initialised")
        }
        val rootDir = File(config.dir1)
        val walk = Walk()
        walk.dir.v(rootDir.absolutePath)
        walk.identifier.v(config.walkIdentifier)
        dao.insertWalk(walk)
        val walkId = walk.id.v()

        val whiteList = setOf("jpg", "jpeg", "png", "bmp", "gif", "dng", "raw", "mp4", "psd")
        val t1 = OTimer("relativeTo")
        val t2 = OTimer("insert")
        /**
         * select w.id,w.c,w.ext, w.hash, w.name, ww.name, ww.ext from
         * 	(select w.id, w.name, w.hash, w.ext, count(1) as c from walkfiles w where w.walkid=1 and w.ext is not null GROUP by w.hash) w
         * 	left join walkfiles ww on (ww.hash=w.hash and ww.id!=w.id)
         * 	where ww.ext is not null
         * 	order by w.c desc;
         */
        Lok.debug("filling db with file entries")
        this.sql.beginTransaction()
        if (true)
            rootDir.walkTopDown().filter { it.isFile }.forEach { file ->
                if (!config.whiteList || file.extension.lowercase() in whiteList) {
                    val f = file.relativeTo(rootDir)
                    try {
                        val entry = WalkerFileEntry()
//                    entry.hash.v(if (config.saveHash) file.inputStream().use { Hash.md5(it) } else null)
                        entry.walkId.v(walkId)
                        entry.path.v(f.parentFile?.path)
                        entry.extension.v(f.extension.ifEmpty { null })
                        entry.name.v(f.nameWithoutExtension)
                        entry.modified.v(file.lastModified() / 1000)
                        entry.size.v(file.length())
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
        Lok.debug("hashing...")
        val walkEntries = dao.loadWalkList(walkId)
        runBlocking {
            walkEntries.forEach { entry ->
                launch(Dispatchers.Default) {
                    val name = if (entry.extension.isNull) entry.name.v() else "${entry.name.v()}.${entry.extension.v()}"
                    val file = if (entry.path.isNull) File(rootDir, name) else File(File(rootDir, entry.path.v()), name)
                    entry.hash.v(if (config.saveHash) file.inputStream().use { Hash.md5(it) } else null)
                }
            }
        }
        Lok.debug("updating db")
        this.sql.beginTransaction()
        walkEntries.forEach { dao.update(it) }
        this.sql.commit()
        t1.print()
        t2.print()
        Lok.debug("done")
    }

}

