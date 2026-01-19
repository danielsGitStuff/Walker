package de.mel.sql;


import de.mel.sql.conn.JDBCConnection;
import de.mel.sql.conn.SQLConnection;
import de.mel.sql.transform.NumberTransformer;
import de.mel.sql.transform.SqlResultTransformer;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * builds and executes mysql queries for several purposes.
 *
 * @author xor
 */
public class SQLQueries extends ASQLQueries implements AutoCloseable {
    @Override
    public void enableWAL() throws SqlQueriesException {
        try {
            PreparedStatement stmt = connection.prepareStatement("PRAGMA journal_mode=WAL");
            stmt.execute();
            stmt.close();
        } catch (SQLException e) {
            throw new SqlQueriesException(e);
        }
    }

    private RWLock lock;
    public static final boolean SYSOUT = false;
    private final JDBCConnection sqlConnection;
    private Connection connection;
    private final SqlResultTransformer resultTransformer;


    public SQLQueries(JDBCConnection connection, SqlResultTransformer resultTransformer) {
        this.sqlConnection = connection;
        this.resultTransformer = resultTransformer;
        this.connection = sqlConnection.getConnection();
    }

    public SQLQueries(JDBCConnection connection, boolean reentrantLockOnWrite, RWLock lock, SqlResultTransformer resultTransformer) {
        this.sqlConnection = connection;
        this.resultTransformer = resultTransformer;
        //todo refine, get rid of lock. this is done by T
//        this.reentrantWriteLock = reentrantLockOnWrite ? new ReentrantLock(true) : null;
        this.connection = sqlConnection.getConnection();
        this.lock = lock;
    }

    @Override
    public SQLConnection getSQLConnection() {
        return sqlConnection;
    }

    @Override
    public void update(SQLTableObject sqlTableObject, String where, List<Object> whereArgs) throws SqlQueriesException {
        lockWrite();
        out("update()");
        String query;
        List<Pair<?>> what = sqlTableObject.getInsertAttributes();
        String fromTable = sqlTableObject.getTableName();
        query = buildInsertModifyQuery(what, where, fromTable);
        out("update().query= " + query);
        try {
            PreparedStatement pstmt = connection.prepareStatement(query);
            int count = 1;
            for (Pair<?> attribute : what) {
                pstmt.setObject(count, attribute.v());
                count++;
            }
            if (where != null && whereArgs != null) {
                insertArguments(pstmt, whereArgs, count);
            }
            int changed = pstmt.executeUpdate();
            pstmt.close();
        } catch (SQLException e) {
            System.err.println(e.getSQLState());
            throw new SqlQueriesException(e);
        } finally {
            unlockWrite();
        }
    }

    @Override
    public void delete(SQLTableObject sqlTableObject, String where, List<Object> whereArgs) throws SqlQueriesException {
        lockWrite();
        String query = "delete from " + sqlTableObject.getTableName();
        if (where != null)
            query += " where " + where;
        out("delete().query= " + query);
        try {
            PreparedStatement pstmt = connection.prepareStatement(query);
            if (where != null && whereArgs != null) {
                insertArguments(pstmt, whereArgs);
            }
            pstmt.executeUpdate();
            pstmt.close();
        } catch (Exception e) {
            throw new SqlQueriesException(e);
        } finally {
            unlockWrite();
        }
    }


    private void insertArguments(PreparedStatement preparedStatement, List<Object> whereArgs, int count) throws SQLException {
        for (Object o : whereArgs) {
            preparedStatement.setObject(count, o);
            count++;
        }
    }


    private void insertArguments(PreparedStatement preparedStatement, Object[] whereArgs, int count) throws SQLException {
        for (Object o : whereArgs) {
            preparedStatement.setObject(count, o);
            count++;
        }
    }

    private void insertArguments(PreparedStatement preparedStatement, List<Object> whereArgs) throws SQLException {
        insertArguments(preparedStatement, whereArgs, 1);
    }

    @Override
    public <T extends SQLTableObject> ISQLResource<T> loadResource(List<Pair<?>> columns, Class<T> clazz, String where,
                                                                   List<Object> whereArgs) throws SqlQueriesException {
        String selectString = ASQLQueries.buildQueryFrom(columns, clazz, where);
        if (connection == null) {
            return null;
        }
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(selectString);
            if (where != null && whereArgs != null) {
                insertArguments(preparedStatement, whereArgs);
            }
            preparedStatement.execute();
            return new SQLResource<>(preparedStatement, clazz, columns);
        } catch (Exception e) {
            throw new SqlQueriesException(e);
        }
    }


    @Override
    public <T extends SQLTableObject> List<T> load(List<Pair<?>> columns, T sqlTableObject, String where, List<Object> arguments) throws SqlQueriesException {
        return load(columns, sqlTableObject, where, arguments, null);
    }

    @Override
    public <T> List<T> loadColumn(Pair<T> column, Class<T> clazz, SQLTableObject sqlTableObject, String tableReference, String where, List<Object> arguments, String whatElse) throws SqlQueriesException {
        List<T> result = new ArrayList<>();
        out("load()");
        String fromTable = sqlTableObject.getTableName();
        if (tableReference != null)
            fromTable += " " + tableReference;
        String selectString = buildSelectQuery(new ArrayList<Pair<?>>() {
            {
                add(column);
            }
        }, fromTable);
        if (where != null) {
            selectString += " where " + where;
        }
        if (whatElse != null) {
            selectString += " " + whatElse;
        }
        out(selectString);
        if (connection == null) {
            return null;
        }
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(selectString);
            if (arguments != null) {
                insertArguments(preparedStatement, arguments);
            }
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();
            boolean hasResult = resultSet.next();
            if (hasResult && resultSet.getRow() > 0) {
                while (!resultSet.isAfterLast()) {
                    try {
                        Object res = resultSet.getObject(column.k());
                        // cast numbers because that does not happen automagically
                        if (Number.class.isAssignableFrom(clazz) && res instanceof Number) {
                            Number casted = NumberTransformer.forType((Class<? extends Number>) clazz).cast((Number) res);
                            result.add((T) casted);
                        } else
                            result.add((T) res);
                    } catch (Exception e) {
                        if (!e.getClass().equals(SQLException.class)) {
                            out("load().exception." + e.getClass().toString() + " " + e.getMessage());
                        }
                    }
                    resultSet.next();
                }
            }
            resultSet.close();
            preparedStatement.close();
            return result;
        } catch (Exception e) {
            System.err.println("SQLQueries.loadColumn.failed.query: " + selectString);
            throw new SqlQueriesException(e);
        }
    }

    @Override
    public <T> List<T> loadColumn(Pair<T> column, Class<T> clazz, String query, List<Object> whereArgs) throws SqlQueriesException {
        List<T> list = new ArrayList<>();
        if (connection == null) {
            return null;
        }
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            if (whereArgs != null) {
                insertArguments(preparedStatement, whereArgs);
            }
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();
            boolean hasResult = resultSet.next();
            if (hasResult && resultSet.getRow() > 0) {
                while (!resultSet.isAfterLast()) {
                    try {
                        Object res = resultSet.getObject(1);
                        // cast numbers because that does not happen automagically
                        if (Number.class.isAssignableFrom(clazz) && res instanceof Number) { //NOSONAR
                            Number casted = NumberTransformer.forType((Class<? extends Number>) clazz).cast((Number) res);
                            list.add((T) casted);
                        } else
                            list.add((T) res);
                    } catch (Exception e) {
                        if (!e.getClass().equals(SQLException.class)) {
                            out("load().exception." + e.getClass().toString() + " " + e.getMessage());
                        }
                    }
                    resultSet.next();
                }
            }
            resultSet.close();
            preparedStatement.close();
        } catch (Exception e) {
            System.err.println("SQLQueries.loadColumn.failed.query: " + query);
            throw new SqlQueriesException(e);
        }
        return list;
    }

    @Override
    public <T extends SQLTableObject> List<T> load(List<Pair<?>> columns, T sqlTableObject, String where, List<Object> arguments, String whatElse) throws SqlQueriesException {
        List<T> result = new ArrayList<>();
        out("load()");
        String fromTable = sqlTableObject.getTableName();
        String selectString = buildSelectQuery(columns, fromTable);
        if (where != null) {
            selectString += " where " + where;
        }
        if (whatElse != null) {
            selectString += " " + whatElse;
        }
        out(selectString);
        if (connection == null) {
            return null;
        }
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(selectString);
            if (arguments != null) {
                insertArguments(preparedStatement, arguments);
            }
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();
            boolean hasResult = resultSet.next();
            if (hasResult && resultSet.getRow() > 0) {
                while (!resultSet.isAfterLast()) {
                    SQLTableObject sqlTable = sqlTableObject.getClass().newInstance();
                    List<Pair<?>> attributes = sqlTable.getAllAttributes();
                    for (Pair<?> pair : attributes) {
                        try {
                            Object res = resultSet.getObject(pair.k());
                            pair.setValueUnsecure(res);
                        } catch (Exception e) {
                            if (!e.getClass().equals(SQLException.class)) {
                                out("load().exception." + e.getClass().toString() + " " + e.getMessage());
                                System.err.println("SQLQueries.load.Exception on setting Pair: " + pair.k());
                            }
                        }
                    }
                    result.add((T) sqlTable);
                    resultSet.next();
                }
            }
            resultSet.close();
            preparedStatement.close();
            return result;
        } catch (Exception e) {
            System.err.println("SqlQieries.load.failed for table     '" + sqlTableObject.getTableName() + "'");
            System.err.println("SqlQieries.load.failed for select     '" + selectString + "'");
            System.err.println("SqlQieries.load.failed for where     '" + (where == null ? "null" : where) + "'");
            System.err.println("SqlQieries.load.failed for whereargs '" + whereArgsToString(arguments) + "'");
            System.err.println("SqlQieries.load.failed for whatelse  '" + (whatElse == null ? "null" : whatElse) + "'");
            System.err.println("SqlQieries.load.failed message       '" + e.getMessage() + "'");
            throw new SqlQueriesException(e);
        }
    }

    @Override
    public <T extends SQLTableObject> List<T> loadString(List<Pair<?>> columns, T sqlTableObject,
                                                         String selectString, List<Object> arguments) throws SqlQueriesException {
        lockRead();
        ArrayList<T> result = new ArrayList<>();
        out("loadString()");
        out(selectString);
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(selectString);
            if (arguments != null) {
                int count = 1;
                for (Object object : arguments) {
                    preparedStatement.setObject(count, object);
                    count++;
                }
            }
            preparedStatement.execute();
            ResultSet resultSet = preparedStatement.getResultSet();
            while (resultSet.next() && !resultSet.isAfterLast()) {
                T sqlObjInstance = (T) sqlTableObject.getClass().newInstance();
                List<Pair<?>> attributes = sqlObjInstance.getAllAttributes();
                for (Pair<?> pair : attributes) {
                    Object res = resultSet.getObject(pair.k());
                    pair.setValueUnsecure(res);
                }
                result.add(sqlObjInstance);
            }
            resultSet.close();
            preparedStatement.close();
            return result;
        } catch (Exception e) {
            throw new SqlQueriesException(e);
        } finally {
            unlockRead();
        }
    }

    @Override
    public <T> T queryValue(String query, Class<T> clazz) throws SqlQueriesException {
        return queryValue(query, clazz, null);
    }

    /**
     * @param query
     * @return true if the first result is a ResultSet object; false if the
     * first result is an update count or there is no result
     */
    @Override
    public <T> T queryValue(String query, Class<T> clazz, List<Object> args) throws SqlQueriesException {
        lockRead();
        Object result = null;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            if (args != null) {
                int count = 1;
                for (Object arg : args) {
                    preparedStatement.setObject(count, arg);
                    count++;
                }
            }
            result = preparedStatement.execute();
            if ((boolean) result) {
                ResultSet resultSet = preparedStatement.getResultSet();
                resultSet.next();
                if (resultSet.getRow() > 0) {
                    //String columnName = resultSet.getMetaData().getColumnLabel(1);
                    result = resultSet.getObject(1);
                    resultSet.close();
                    preparedStatement.close();
                }
            } else {
                preparedStatement.close();
                return null;
            }
        } catch (Exception e) {
            throw new SqlQueriesException(e);
        } finally {
            unlockRead();
        }
        return resultTransformer.convert(clazz, result);
    }


    /**
     * see loadString... duplicate? nope
     *
     * @param statement
     * @param whereArgs
     * @return
     * @throws SqlQueriesException
     */
    @Override
    public void execute(String statement, List<Object> whereArgs) throws SqlQueriesException {
        lockRead();
        try {
            out("execute.stmt: " + statement);
            String attrs = "";
            if (whereArgs != null)
                for (Object o : whereArgs) {
                    attrs += (o == null ? "null" : o.toString()) + ", ";
                }
            out("execute.attr: " + attrs);
            PreparedStatement preparedStatement = connection.prepareStatement(statement);
            if (whereArgs != null && whereArgs != null) {
                insertArguments(preparedStatement, whereArgs, 1);
            }
            preparedStatement.execute();
            preparedStatement.close();
        } catch (Exception e) {
            System.err.println("SQLQueries.execute.stmt: " + statement);
            System.err.println("SQLQueries.execute.args : " + whereArgsToString(whereArgs));
            throw new SqlQueriesException(e);
        } finally {
            unlockRead();
        }
    }

    private String whereArgsToString(List<Object> whereArgs) {
        if (whereArgs == null)
            return "null";
        StringBuilder attrs = new StringBuilder("[");
        for (Object arg : whereArgs) {
            attrs.append(arg == null ? "null" : arg.toString()).append(", ");
        }
        attrs.append("]");
        return attrs.toString();
    }


    @Override
    public Long insert(SQLTableObject sqlTableObject) throws SqlQueriesException {
        Long id = insertWithAttributes(sqlTableObject, sqlTableObject.getInsertAttributes());
        sqlTableObject.onInsert(id);
        return id;
    }

    @Override
    public Long insertWithAttributes(SQLTableObject sqlTableObject, List<Pair<?>> attributes) throws SqlQueriesException {
        lockWrite();
        out("insert()");
        StringBuilder query = null;
        String fromTable = sqlTableObject.getTableName();
        if (attributes == null) {
            System.err.println("SQLQueries.insertWithAttributes: attributes are null.");
            System.err.println("SQLQueries.insertWithAttributes: have you called init() in the constructor of " + sqlTableObject.getClass().getSimpleName() + "?");
        }
        try {
            query = new StringBuilder(" insert into " + fromTable + " (");
            String toConcat = ") values (";
            for (int i = 0; i < attributes.size(); i++) {
                String key = attributes.get(i).k();
                if (i < attributes.size() - 1) {
                    query.append(key).append(", ");
                    toConcat += " ? , ";
                } else {
                    query.append(key);
                    toConcat += " ? ";
                }
            }
            query.append(toConcat).append(")");
//            out("insert.query: " + query);
//            out("insert.attributes: ");
//            for (Pair pair : attributes) {
//                System.out.print(pair.v() + ", ");
//            }
        } catch (Exception e) {
            throw new SqlQueriesException(e);
        }
        try {

            PreparedStatement preparedStatement = connection.prepareStatement(query.toString(), Statement.RETURN_GENERATED_KEYS);
            for (int i = 1; i <= attributes.size(); i++) {
                Pair<?> attribute = attributes.get(i - 1);
                preparedStatement.setObject(i, attribute.v());
            }
            if (reentrantWriteLock != null)
                reentrantWriteLock.lock();
            preparedStatement.executeUpdate();
            ResultSet resultSet = preparedStatement.getGeneratedKeys();
            resultSet.next();
            if (resultSet.getRow() > 0) {
                Object id = resultSet.getObject(1);
                resultSet.close();
                preparedStatement.close();
                if (reentrantWriteLock != null)
                    reentrantWriteLock.unlock();
                if (id instanceof Integer)
                    return Long.valueOf((Integer) id);
                if (id instanceof Long)
                    return (Long) id;
            }
        } catch (Exception e) {
            //e.printStackTrace();
            if (reentrantWriteLock != null)
                reentrantWriteLock.unlock();
            System.err.println("SQLQueries.insert.excep: " + e.getClass().getSimpleName() + ", msg: " + e.getMessage());
            System.err.println("SQLQueries.insert.query: " + query);
            StringBuilder attrs = new StringBuilder();
            for (Pair pair : attributes) {
                attrs.append(pair.v()).append(", ");
            }
            System.err.println("SQLQueries.insert.attributes: " + attrs.toString());

            throw new SqlQueriesException(e);
        } finally {
            unlockWrite();
        }
        out("insert().doing nothing right now");
        return null;
    }


    @Override
    public void lockRead() {
//        if (lock != null)
//            lock.lockRead();
    }

    @Override
    public void unlockRead() {
//        if (lock != null)
//            lock.unlockRead();
    }

    @Override
    public void lockWrite() {
//        if (lock != null)
//            lock.lockWrite();
    }

    @Override
    public void unlockWrite() {
//        if (lock != null)
//            lock.unlockWrite();
    }


    @Override
    public void beginTransaction() throws SQLException {
        connection.setAutoCommit(false);
    }

    @Override
    public void commit() throws SQLException {
        connection.commit();
        connection.setAutoCommit(true);
    }


    public void rollback() throws SqlQueriesException {
        try {
            connection.rollback();
            connection.setAutoCommit(true);
        } catch (Exception e) {
            throw new SqlQueriesException(e);
        }
    }


    @Override
    public <T extends SQLTableObject> T loadFirstRow(List<Pair<?>> columns, T sqlTableObject, String where, List<Object> whereArgs, Class<T> castClass) throws SqlQueriesException {
        List<T> list = load(columns, sqlTableObject, where, whereArgs, "limit 1");
        if (!list.isEmpty())
            return list.get(0);
        return null;
    }


    @Override
    public void onShutDown() {
        try {
            if (connection != null) {
                connection.close();
                connection = null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws SqlQueriesException {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new SqlQueriesException(e);
        }
    }

    @Override
    public <T extends SQLTableObject> ISQLResource<T> loadQueryResource(String query, List<Pair<?>> columns, Class<T> clazz, List<Object> args) throws SqlQueriesException {
        if (connection == null) {
            return null;
        }
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            if (args != null) {
                insertArguments(preparedStatement, args);
            }
            preparedStatement.execute();
            return new SQLResource<T>(preparedStatement, clazz, columns);
        } catch (Exception e) {
            throw new SqlQueriesException(e);
        }
    }
}
