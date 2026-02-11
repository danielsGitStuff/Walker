package de.walker.db

import de.mel.core.serialize.serialize.tools.OTimer
import de.mel.sql.SQLQueries
import kotlinx.coroutines.channels.Channel

class DbBatchWriter<T>(
    val name: String, val sql: SQLQueries, val batchSize: Int = 1000, val process: (T) -> Unit
) {
    val timerIterate: OTimer = OTimer("iterate")
    val timerCommit: OTimer = OTimer("commit")
    suspend fun consume(channel: Channel<T>) {
        val batch = ArrayList<T>(batchSize)

        fun flush() {
            if (batch.isEmpty()) return
            println("[${name}] Writing ${batch.size} records.")
            sql.beginTransaction()
            try {
                timerCommit.reset()
                timerIterate.reset().start()
                batch.forEach { process(it) }
                timerIterate.stop()
                timerCommit.start()
                sql.commit()
                timerCommit.stop()
                println("Iterate took ${timerIterate.durationInMS}, Commit took ${timerCommit.durationInMS} ms.")
            } catch (e: Exception) {
                sql.rollback()
                println("Batch failed: $e")
            }
            batch.clear()
        }

        for (item in channel) {
            batch.add(item)
            if (batch.size >= batchSize) flush()
        }
        flush()
    }
}