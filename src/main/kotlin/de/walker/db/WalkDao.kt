package de.walker.db

import de.mel.sql.Dao
import de.mel.sql.ISQLQueries

class WalkDao(sql: ISQLQueries) : Dao(sql) {
    companion object {
        fun createSql(): String = """
    create table if not exists walk(
    id integer primary key autoincrement,
    dir text not null
    );

    create table if not exists walkfiles(
    id integer primary key autoincrement,
    walkid integer not null,
    path text,
    name text not null,
    ext text not null,
    hash text,
    modified int not null,
    created int not null,
    foreign key (walkid) references walk (id)
    );

    create index ientry on walkfiles(hash, walkid, ext);"""
    }

    fun insertWalk(walk: Walk): Walk {
        val id = sqlQueries.insert(walk)
        walk.id.v(id)
        return walk
    }

    fun insertFilEntry(entry: WalkerFileEntry): WalkerFileEntry {
        val id = sqlQueries.insert(entry)
        entry.id.v(id)
        return entry
    }


}