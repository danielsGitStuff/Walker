package de.walker

import de.mel.core.serialize.serialize.tools.OTimer
import de.mel.konsole.Konsole

fun main(args: Array<String>) {
    val config = Config()
    val k = Konsole(config)
    k.mandatory("-d1", "directory 1") { result, args -> result.dir1 = args[0] }
    k.optional("-db", "set db file") { result, args -> result.dbFile = args[0] }
    k.optional("-wl", "enable white list file extension mode") { result, args -> result.whiteList = true }
    k.optional("-hash", "save hashes too") { result, args -> result.saveHash = true }
    k.optional("-threads", "sets the number of threads used.") { result, args -> result.threadsMax = args[0].toInt() }
    k.optional("-hdd", "disables multithreading for HDDs.") { result, args ->
        result.threadsMax = 1
    }
    k.handle(args)
    println(config.dir1)
    val timer = OTimer("overall").start()
    val walker = Walker(config)
    walker.start()
    timer.stop().print()
}