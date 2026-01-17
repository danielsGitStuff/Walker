package de.walker.db

import de.mel.sql.SQLQueries
import kotlinx.coroutines.channels.Channel

class DbBatchWriter<T>(
    val sql: SQLQueries,
    val batchSize: Int = 1000,
    val process: (T) -> Unit
) {
    suspend fun consume(channel: Channel<T>) {
        val batch = ArrayList<T>(batchSize)

        fun flush() {
            if (batch.isEmpty()) return
//            sql.beginTransaction()
            try {
                batch.forEach { process(it) }
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