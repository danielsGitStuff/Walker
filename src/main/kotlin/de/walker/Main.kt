package de.walker

import de.mel.core.serialize.serialize.tools.OTimer
import de.mel.konsole.Konsole

fun main(args: Array<String>) {
    val config = Config()
    val k = Konsole(config)
    k.positional("walk identifier", "unique identifier of the filewalk") { result, args -> result.walkIdentifier = args[0] }
    k.positional("mode", "you can 'index' or 'deduplicate'") { result, args ->
        result.mode = when (args[0]) {
            "index" -> Mode.INDEX
            "deduplicate" -> Mode.DEDUPLICATE
            else -> throw RuntimeException("NO VALID MODE! '${args[0]}'")
        }
        result.mode = if (args[0] == "index") Mode.INDEX else Mode.DEDUPLICATE
    }
    k.mandatory("-d1", "directory 1") { result, args -> result.dir1 = args[0] }
    k.optional("-db", "set db file") { result, args -> result.dbFile = args[0] }
    k.optional("-wl", "enable white list file extension mode") { result, args -> result.whiteList = true }
    k.optional("-hash", "save hashes too") { result, args -> result.saveHash = true }
    k.optional("-dedup", "deduplicate files") { result, args -> result.deduplicateFiles = true }
    k.handle(args)
    println(config.dir1)
    val timer = OTimer("overall").start()
    val walker = Walker(config)
    walker.start()
    timer.stop().print()
}