package de.walker

import de.mel.KResult

enum class Mode {
    INDEX, DEDUPLICATE
}

class Config : KResult {
    lateinit var walkIdentifier: String
    lateinit var mode: Mode
    lateinit var dir1: String
    var dbFile = "walker.db"
    var saveHash = false
    var whiteList = false
    var deduplicateFiles = false
}