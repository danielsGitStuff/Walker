package de.walker

import de.mel.core.serialize.serialize.tools.OTimer
import de.mel.execute.SqliteExecutor
import de.mel.sql.*
import de.walker.db.Walk
import de.walker.db.WalkDao
import de.walker.db.WalkerFileEntry
import java.io.File
import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes
import kotlin.io.path.fileSize
import de.mel.sql.SqliteQueriesCreator
import de.walker.db.DbBatchWriter
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

class Walker(val config: Config) {




    fun start() = runBlocking {
        SqliteQueriesCreator.createSqliteQueries(File(config.dbFile)).use { sql ->
            val dao = WalkDao(sql)
            sql.execute("PRAGMA foreign_keys = ON")
            sql.enableWAL()
            val ex = SqliteExecutor(sql.sqlConnection)
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

            val t1 = OTimer("relativeTo")
            val t2 = OTimer("insert")

            sql.beginTransaction()
            indexMetadata(rootDir, walkId, dao, sql)

            sql.commit()
            if (config.saveHash){
                sql.beginTransaction()
                calculateHashes(walkId, dao, sql)
            }

            sql.commit()
            t1.print()
            t2.print()
        }
    }

    private suspend fun calculateHashes(walkId: Long, dao: WalkDao, sql: SQLQueries) = coroutineScope {
        val entriesToProcess = dao.getEntries(walkId)

        val updateChannel = Channel<WalkerFileEntry>(capacity = 100) // ID -> Hash
        val inputChannel = Channel<WalkerFileEntry>(capacity = 100)

        // Updates DB
        val writerJob = launch(Dispatchers.IO) {
            DbBatchWriter<WalkerFileEntry>(sql, batchSize = 500) { entry ->
                dao.update(entry)
            }.consume(updateChannel)
        }

        // 2. The Heavy Workers
        val workers = List(Runtime.getRuntime().availableProcessors()) {
            launch(Dispatchers.IO) {
                for (entry in inputChannel) {
                    try {
                        val name = "${entry.path.v()}/${entry.name.v()}" + (entry.extension.v()?.let { ".$it" } ?: "")
                        val fullPath = File(config.dir1, name)

                        // HEAVY IO
                        val hash = fullPath.inputStream().use { Hash.md5(it) }
                        entry.hash.v(hash)

                        updateChannel.send(entry)
                    } catch (e: Exception) {
                        println("Hash error: $e")
                    }
                }
            }
        }


        launch(Dispatchers.Default) {
            entriesToProcess.use { entries->
                var next = entries.next
                while (next != null) {
                    inputChannel.send(next)
                    next = entries.next()
                }
            }
            inputChannel.close()
        }

        // Cleanup
        workers.joinAll()
        updateChannel.close()
        writerJob.join()
    }

    private suspend fun indexMetadata(rootDir: File, walkId: Long, dao: WalkDao, sql: SQLQueries) = coroutineScope {
        val entryChannel = Channel<WalkerFileEntry>(capacity = 2048)
        val whiteList = setOf("jpg", "jpeg", "png", "bmp", "gif", "dng", "raw", "mp4", "psd", "webp")


        val writerJob = launch(Dispatchers.IO) {
            DbBatchWriter<WalkerFileEntry>(sql, batchSize = 2048) { entry ->
                dao.insertFilEntry(entry)
            }.consume(entryChannel)
        }

        launch(Dispatchers.IO) {
            rootDir.walkTopDown()
                .filter { it.isFile }
                .forEach { file ->
                    if ((!config.whiteList || file.extension.lowercase() in whiteList) && !Files.isSymbolicLink(file.toPath())) {
                        try {
                            val f = file.relativeTo(rootDir)
                            val entry = WalkerFileEntry()

                            // FAST operations only
                            entry.walkId.v(walkId)
                            entry.path.v(f.parentFile?.path) // relative path
                            entry.name.v(f.nameWithoutExtension)
                            entry.extension.v(f.extension.takeIf { it.isNotBlank() })
                            entry.size.v(file.length()) // Standard Java API is fast
                            entry.modified.v(file.lastModified() / 1000)

                            // Slightly slower, but necessary for 'created'
                            val attrs = Files.readAttributes(file.toPath(), BasicFileAttributes::class.java)
                            entry.created.v(attrs.creationTime().toMillis() / 1000)

                            entryChannel.send(entry)
                        } catch (e: Exception) {
                            println("Meta error: ${file.path}")
                        }
                    }
                }
            entryChannel.close()
        }
        writerJob.join()
    }
}

