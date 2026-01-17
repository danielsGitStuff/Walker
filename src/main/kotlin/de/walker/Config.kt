package de.walker

import de.mel.KResult

class Config : KResult {
    lateinit var dir1: String
    var dbFile = "walker.db"
    var saveHash = false
    var whiteList = false
    var hdd = false
    var threadsMax = Runtime.getRuntime().availableProcessors()
}