package de.walker.db

import de.mel.sql.Dao
import de.mel.sql.ISQLQueries
import de.mel.sql.ISQLResource

class WalkDao(sql: ISQLQueries) : Dao(sql) {
    companion object {
        fun createSql(): String = """
    create table if not exists walk(
    id integer primary key autoincrement,
    identifier text not null unique,
    dir text not null
    );

    create table if not exists walkfiles(
    id integer primary key autoincrement,
    walkid integer not null,
    path text,
    name text not null,
    ext text,
    hash text,
    size int not null,
    modified int not null,
    created int not null,
    foreign key (walkid) references walk (id)
    );

    create index ientry on walkfiles(hash, walkid, ext);
    create index ihash on walkfiles(hash);"""

        val dummyWalkEntry = WalkerFileEntry()
        val dummyDedup = DedupEntry()
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

    fun loadWalk(walkId: Long): ISQLResource<out WalkerFileEntry> {
        val where = "${dummyWalkEntry.walkId.k()}=?"
        return sqlQueries.loadResource(dummyWalkEntry.allAttributes, dummyWalkEntry::class.java, where, listOf(walkId))
    }

    fun loadWalkList(walkId: Long): List<WalkerFileEntry> {
        val where = "${dummyWalkEntry.walkId.k()}=?"
        return sqlQueries.load(dummyWalkEntry.allAttributes, dummyWalkEntry, where, listOf(walkId))
    }

    fun update(walkerFileEntry: WalkerFileEntry) {
        sqlQueries.update(walkerFileEntry, "${walkerFileEntry.id.k()}=?", listOf(walkerFileEntry.id.v()))
    }

    fun findDebug(walkIdentifier: String): List<DedupEntry> {
//        select w.id,w.c,w.ext, w.hash, w.name, ww.name, ww.ext from
//        (select w.id, w.name, w.hash, w.ext, count(1) as c from walkfiles w where w.walkid=1 and w.ext is not null GROUP by w.hash) w
//        left join walkfiles ww on (ww.hash=w.hash and ww.id!=w.id)
//        where ww.ext is not null
//        order by w.c desc;
        val W = dummyWalkEntry
        val WA = Walk()
        val query =
            """select w.${W.id.k()} as id, w.${W.path.k()} as srcpath, w.${W.name.k()} as srcname, w.${W.extension.k()} as srcext, w.c as c
                , w.${W.hash.k()} as srchash
                , ww.${W.path.k()} as otherpath, ww.${W.name.k()} as othername, ww.${W.extension.k()} as otherext from
         (select w.${W.id.k()}, w.${W.name.k()}, w.${W.hash.k()}, w.${W.path.k()}, w.${W.size.k()}, w.${W.extension.k()}, count(1) as c from ${dummyWalkEntry.tableName} w 
         where w.${W.walkId.k()}=(select ${WA.id.k()} from ${WA.tableName} where ${WA.identifier.k()}=?) 
         and w.${W.extension.k()} is not null GROUP by w.${W.hash.k()}) w
         left join ${W.tableName} ww on (ww.${W.hash.k()} = w.${W.hash.k()} 
         and ww.${W.id.k()} != w.${W.id.k()} 
         and ww.${W.extension.k()} = w.${W.extension.k()})
         where ww.${W.extension.k()} is not null and w.c > 1 and ww.${W.size.k()} > 0 and w.${W.size.k()} > 0
         order by w.c desc;"""
        return sqlQueries.loadQueryResource(query, dummyDedup.allAttributes, dummyDedup::class.java, listOf(walkIdentifier)).toList()
//        return sqlQueries.load(dummyDedup.allAttributes, dummyDedup,null, listOf())
    }

}