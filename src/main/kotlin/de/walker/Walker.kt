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
import kotlinx.coroutines.flow.buffer
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
            sql.execute("PRAGMA cache_size = -200000")
            sql.execute("PRAGMA synchronous = NORMAL")
            sql.execute("PRAGMA wal_autocheckpoint = 25000")
            sql.execute("PRAGMA temp_store = MEMORY")
            sql.execute("PRAGMA mmap_size = 30000000000")
            sql.execute("PRAGMA busy_timeout = 30000")
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
            println("indexing ...")
            indexMetadata(rootDir, walkId, dao, sql)
            tInit.stop().print()

            if (config.saveHash) {
                println("hashing ...")
                tHash?.start()
                sql.beginTransaction()
                calculateHashes(walkId, dao, sql)
            }
            tInit.print()
            tHash?.stop()?.print()
        }
    }

    private suspend fun calculateHashes(walkId: Long, dao: WalkDao, sql: SQLQueries) = coroutineScope {
        val entriesToProcess = dao.getEntriesResource(walkId)
        val entriesCount = dao.countEntries(walkId)
        val progressCounter = java.util.concurrent.atomic.AtomicLong(0)

        val updateChannel = Channel<WalkerFileEntry>(capacity = 4096) // ID -> Hash
        val inputChannel = Channel<WalkerFileEntry>(capacity = 4096)

        // Updates DB
        val writerJob = launch(Dispatchers.IO) {
            DbBatchWriter<WalkerFileEntry>("hash", sql, batchSize = 1024) { entry ->
                dao.update(entry)
            }.consume(updateChannel)
        }

        // 2. The Heavy Workers
        val walkPath = sql.queryValue(
            "select ${Walk.DIR} from ${Walk().tableName} where ${Walk.ID} = ?",
            String::class.java,
            ASQLQueries.args(walkId)
        )
        val bytesToHash = sql.queryValue(
            "select sum(size) from ${WalkerFileEntry().tableName} where ${WalkerFileEntry.WALKID} = ? and hash is null",
            Long::class.java,
            ASQLQueries.args(walkId)
        )
        val totalBytes = bytesToHash ?: 1L
        val bytesProcessed = java.util.concurrent.atomic.AtomicLong(0)
        val monitorJob = launch(Dispatchers.Default) {
            var lastBytes = 0L
            var lastTime = System.currentTimeMillis()

            while (isActive) {
                delay(30_000) // Update every 30 seconds

                val currentBytes = bytesProcessed.get()
                val currentTime = System.currentTimeMillis()
                val currentFiles = progressCounter.get()

                // Calculate Delta (Windowed Speed)
                val bytesDelta = currentBytes - lastBytes
                val timeDelta = currentTime - lastTime

                // Avoid division by zero
                if (timeDelta > 0) {
                    val speedBytesPerMs = bytesDelta.toDouble() / timeDelta
                    val speedMBps = (speedBytesPerMs * 1000) / (1024 * 1024)

                    val percent = (currentBytes.toDouble() / totalBytes) * 100.0
                    val remainingBytes = totalBytes - currentBytes

                    // Calc ETA
                    val etaSeconds =
                        if (speedBytesPerMs > 0) (remainingBytes / (speedBytesPerMs * 1000)).toLong() else 0
                    val etaString =
                        String.format("%02d:%02d:%02d", etaSeconds / 3600, (etaSeconds % 3600) / 60, etaSeconds % 60)

                    println(
                        String.format(
                            "[Progress] %.2f%% done | Speed: %.2f MB/s | ETA: %s | Total: %.2f GB. Files hashed: %d / %d.",
                            percent,
                            speedMBps,
                            etaString,
                            currentBytes.toDouble() / (1024.0 * 1024.0 * 1024.0),
                            currentFiles,
                            entriesCount
                        )
                    )
                }

                lastBytes = currentBytes
                lastTime = currentTime
            }
        }


        val rootDir = File(walkPath)
        val workers = List(config.threadsMax) {
            launch(Dispatchers.IO) {
                val hasher = Hash(1024 * 32)
                for (entry in inputChannel) {
                    try {
                        val dir = if (entry.path.isNull) rootDir else File(rootDir, entry.path.v())
                        val name = "${entry.name.v()}" + (entry.extension.v()?.let { ".$it" } ?: "")
                        val fullPath = File(dir, name)

                        // HEAVY IO
                        val hash = fullPath.inputStream().use { hasher.md5l(it) }
                        entry.hash.v(hash)
                        val fileSize = entry.size.v() ?: 0L
                        bytesProcessed.addAndGet(fileSize)
                        progressCounter.incrementAndGet()
//                        if (progressCounter.incrementAndGet() % 10000L == 0L) {
//                            println("${progressCounter.get()}/${entriesCount} hashed.")
//                        }

                        updateChannel.send(entry)
                    } catch (e: Exception) {
                        println("Hash error: $e")
                        dao.logHashException(entry, e)
                    }
                }
            }
        }

        entriesToProcess.use { entries ->
            var counter = 1L
            var walkerFileEntry = entries.next()
            while (walkerFileEntry != null) {
                inputChannel.send(walkerFileEntry)
                walkerFileEntry = entries.next()
                counter++
            }
        }
        inputChannel.close()

        // Cleanup
        workers.joinAll()
        monitorJob.cancel()
        updateChannel.close()
        writerJob.join()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun indexMetadata(rootDir: File, walkId: Long, dao: WalkDao, sql: SQLQueries) = coroutineScope {
        val entryChannel = Channel<WalkerFileEntry>(capacity = 2048)
        val whiteList = setOf(
            "jpg",
            "jpeg",
            "png",
            "bmp",
            "gif",
            "dng",
            "raw",
            "mp4",
            "psd",
            "webp",
            "mp3",
            "opus",
            "ogg",
            "wav",
            "flac"
        )


        val writerJob = launch(Dispatchers.IO) {
            DbBatchWriter<WalkerFileEntry>("init", sql, batchSize = 2048 * 10) { entry ->
                dao.insertFilEntry(entry)
            }.consume(entryChannel)
        }

        launch(Dispatchers.IO) {
            val countSeen = java.util.concurrent.atomic.AtomicLong(0)
            val countIndex = java.util.concurrent.atomic.AtomicLong(0)
            rootDir.walkTopDown()
                .onEnter { !Files.isSymbolicLink(it.toPath()) }
                .filter {
                    if (countSeen.incrementAndGet() % 10000L == 0L) {
                        println("Seen: ${countSeen.get()}, indexed: ${countIndex.get()} files.")
                    }
                    return@filter it.isFile
                }
                .filter { !(this@Walker.config.whiteList && !whiteList.contains(it.extension.lowercase())) }
                .onEach { }
                .asFlow()
                .buffer(Channel.UNLIMITED)
                .flatMapMerge(concurrency = config.threadsMax) { file ->
                    flow {
                        try {
                            countIndex.incrementAndGet()
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

        writerJob.join()
    }
}

