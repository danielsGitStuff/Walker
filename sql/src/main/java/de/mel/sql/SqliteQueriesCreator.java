package de.mel.sql;

import de.mel.sql.conn.JDBCConnection;
import de.mel.sql.conn.SQLConnector;
import de.mel.sql.transform.SqlResultTransformer;

import java.io.File;
import java.sql.SQLException;

public class SqliteQueriesCreator {
    /**
     * Convenience function to get sqlite connections
     *
     * @param dbFile where you db is or should be stored
     * @return {@link SQLQueries} or throws RTE
     */
    public static SQLQueries createSqliteQueries(File dbFile) {
        try {
            JDBCConnection sqliteConnection = SQLConnector.createSqliteConnection(dbFile);
            return new SQLQueries(sqliteConnection, SqlResultTransformer.sqliteResultSetTransformer());
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
