package de.walker

import de.mel.core.serialize.serialize.tools.OTimer
import de.mel.execute.SqliteExecutor
import de.mel.sql.ASQLQueries
import de.mel.sql.Hash
import de.mel.sql.SQLQueries
import de.mel.sql.SqliteQueriesCreator
import de.walker.db.DbBatchWriter
import de.walker.db.Walk
import de.walker.db.WalkDao
import de.walker.db.WalkerFileEntry
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import java.io.File
import java.nio.file.Files
import java.nio.file.attribute.BasicFileAttributes

class Walker(val config: Config) {


    fun start() = runBlocking {
        SqliteQueriesCreator.createSqliteQueries(File(config.dbFile)).use { sql ->
            val dao = WalkDao(sql)
            sql.execute("PRAGMA foreign_keys = ON")
            SqliteExecutor(sql.sqlConnection).executeStream(this::class.java.getResourceAsStream("/de/walker/init.sql"))
            sql.enableWAL()

            val rootDir = File(config.dir1)
            val walk = Walk()
            walk.dir.v(rootDir.absolutePath)
            dao.insertWalk(walk)
            val walkId = walk.id.v()

            val tInit = OTimer("initialize").start()
            val tHash = if (config.saveHash) OTimer("hash") else null

            sql.beginTransaction()
            indexMetadata(rootDir, walkId, dao, sql)
            tInit.stop().print()

            sql.commit()
            if (config.saveHash) {
                tHash?.start()
                sql.beginTransaction()
                calculateHashes(walkId, dao, sql)
            }

            sql.commit()
            tInit.print()
            tHash?.stop()?.print()
        }
    }

    private suspend fun calculateHashes(walkId: Long, dao: WalkDao, sql: SQLQueries) = coroutineScope {
        val entriesToProcess = dao.getEntriesResource(walkId)
        val entriesCount = dao.countEntries(walkId)

        val updateChannel = Channel<WalkerFileEntry>(capacity = 100) // ID -> Hash
        val inputChannel = Channel<kotlin.Pair<WalkerFileEntry, Long>>(capacity = 100)

        // Updates DB
        val writerJob = launch(Dispatchers.IO) {
            DbBatchWriter<WalkerFileEntry>(sql, batchSize = 500) { entry ->
                dao.update(entry)
            }.consume(updateChannel)
        }

        // 2. The Heavy Workers
        val walkPath = sql.queryValue(
            "select ${Walk.DIR} from ${Walk().tableName} where ${Walk.ID} = ?",
            String::class.java,
            ASQLQueries.args(walkId)
        )
        val rootDir: File = File(walkPath)
        val workers = List(Runtime.getRuntime().availableProcessors()) {
            launch(Dispatchers.IO) {
                for ((entry, counter) in inputChannel) {
                    try {
                        val dir = if (entry.path.isNull) rootDir else File(rootDir, entry.path.v())
                        val name = "${entry.name.v()}" + (entry.extension.v()?.let { ".$it" } ?: "")
                        val fullPath = File(dir, name)

                        // HEAVY IO
                        val hash = fullPath.inputStream().use { Hash.md5(it) }
                        entry.hash.v(hash)
                        if (counter % 10000L == 0L) {
                            println("${counter}/${entriesCount} hashed.")
                        }

                        updateChannel.send(entry)
                    } catch (e: Exception) {
                        println("Hash error: $e")
                        dao.logHashException(entry, e)
//                        println("name: ${entry.path.v()}/${entry.name.v()}" + (entry.extension.v()?.let { ".$it" } ?: ""))
//                        println("ext null? ${entry.extension.isNull}")
                    }
                }
            }
        }

        entriesToProcess.use { entries ->
            var counter = 1L
            var walkerFileEntry = entries.next
            while (walkerFileEntry != null) {
                inputChannel.send(kotlin.Pair(walkerFileEntry, counter))
                walkerFileEntry = entries.next()
                counter++
            }
        }
        inputChannel.close()

        // Cleanup
        workers.joinAll()
        updateChannel.close()
        writerJob.join()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun indexMetadata(rootDir: File, walkId: Long, dao: WalkDao, sql: SQLQueries) = coroutineScope {
        val entryChannel = Channel<WalkerFileEntry>(capacity = 2048)
        val whiteList = setOf("jpg", "jpeg", "png", "bmp", "gif", "dng", "raw", "mp4", "psd", "webp")


        val writerJob = launch(Dispatchers.IO) {
            DbBatchWriter<WalkerFileEntry>(sql, batchSize = 2048) { entry ->
                dao.insertFilEntry(entry)
            }.consume(entryChannel)
        }

        val fileChannel = Channel

        val workers = List(config.threadsMax) {
            launch(Dispatchers.IO) {

            }
        }

        launch(Dispatchers.IO) {
            rootDir.walkTopDown()
                .filter { it.isFile }
                .asFlow()
                .flatMapMerge(concurrency = config.threadsMax) { file ->
                    flow {
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

                            emit(entry)
                        } catch (e: Exception) {
                            println("Meta error: ${file.path}")
                            println(e)
                        }
                    }.flowOn(Dispatchers.IO)
                }.collect { entryChannel.send(it) }
            entryChannel.close()
        }


//        launch(Dispatchers.IO) {
//            rootDir.walkTopDown()
//                .filter { it.isFile }
//                .forEach { file ->
//                    if ((!config.whiteList || file.extension.lowercase() in whiteList) && !Files.isSymbolicLink(file.toPath())) {
//                        try {
//                            val f = file.relativeTo(rootDir)
//                            val entry = WalkerFileEntry()
//
//                            // FAST operations only
//                            entry.walkId.v(walkId)
//                            entry.path.v(f.parentFile?.path) // relative path
//                            entry.name.v(f.nameWithoutExtension)
//                            entry.extension.v(f.extension.takeIf { it.isNotBlank() })
//                            entry.size.v(file.length()) // Standard Java API is fast
//                            entry.modified.v(file.lastModified() / 1000)
//
//                            // Slightly slower, but necessary for 'created'
//                            val attrs = Files.readAttributes(file.toPath(), BasicFileAttributes::class.java)
//                            entry.created.v(attrs.creationTime().toMillis() / 1000)
//
//                            entryChannel.send(entry)
//                        } catch (e: Exception) {
//                            println("Meta error: ${file.path}")
//                        }
//                    }
//                }
//            entryChannel.close()
//        }
        writerJob.join()
    }
}

