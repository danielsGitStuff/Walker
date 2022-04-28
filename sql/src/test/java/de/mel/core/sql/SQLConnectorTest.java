package de.mel.core.sql;


import de.mel.sql.conn.SQLConnection;
import de.mel.sql.conn.SQLConnector;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

/**
 * Created by xor on 1/7/16.
 */
public class SQLConnectorTest {
    private static File sqliteFile = new File("sqlite.db");

    @Test
    public void sqlite() throws SQLException, ClassNotFoundException {
        SQLConnection c = SQLConnector.createConnection("client");
        System.out.println(c);
    }

    @Before
    public void before() {
        sqliteFile.delete();
    }

//    @Test
//    public void transaction() throws Exception {
//        SQLConnection con = SQLConnector.createSqliteConnection((sqliteFile));
//        SqliteExecutor executor = new SqliteExecutor(con);
//        if (!executor.checkTableExists("atest")) {
//            executor.executeStream(new FileInputStream(new File("/test.sql")));
//        }
//
//        CrashTestDummy dummy = new CrashTestDummy().setName("test 1");
//        CrashTestDummy dummy1 = new CrashTestDummy().setName("test 2");
//        SQLQueries sqlQueries = new SQLQueries(con);
//        sqlQueries.beginTransaction();
//        Long dummyId = sqlQueries.insert(dummy);
//        sqlQueries.rollback();
//        sqlQueries.insert(dummy1);
//        // get stuff out again
//        List<Object> args = new ArrayList<>();
//        args.add(dummyId);
//        List<CrashTestDummy> res = sqlQueries.load(dummy.getAllAttributes(), dummy, dummy.getId().k() + "=?", args);
//        CrashTestDummy out1 = res.get(0);
//        assertEquals(out1.getName().v(), dummy1.getName().v());
//    }
}
