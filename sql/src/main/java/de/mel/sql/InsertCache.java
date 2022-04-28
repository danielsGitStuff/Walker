package de.mel.sql;

import de.mel.DebugTimer;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InsertCache {
    private Map<List<String>, PreparedStatement> statementMap;
    private boolean active;
    private Connection connection;
    public DebugTimer debugTimer = new DebugTimer("insert.debug.timer");

    public InsertCache(Connection connection) {
        this.connection = connection;
        this.active = true;
        this.statementMap = new HashMap<>();
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public boolean isActive() {
        return active;
    }

    private PreparedStatement getInsertStatement(SQLTableObject sqlTableObject, List<Pair<?>> attributes) throws SQLException {
        List<String> statementKey = new ArrayList<>(attributes.size() + 1);
        statementKey.add(sqlTableObject.getTableName());
        for (Pair<?> attr : attributes) {
            statementKey.add(attr.k());
        }
        PreparedStatement preparedStatement;
        if (this.active){
            preparedStatement = this.statementMap.getOrDefault(statementKey, null);
            if (preparedStatement == null) {
                String statement = this.buildStatementStr(sqlTableObject, attributes);
                preparedStatement = connection.prepareStatement(statement, Statement.RETURN_GENERATED_KEYS);
                this.statementMap.put(statementKey, preparedStatement);
            }
        }else {
            String statement = this.buildStatementStr(sqlTableObject, attributes);
            preparedStatement = connection.prepareStatement(statement, Statement.RETURN_GENERATED_KEYS);
            this.statementMap.put(statementKey, preparedStatement);
        }
        return preparedStatement;
    }

    private String buildStatementStr(SQLTableObject sqlTableObject, List<Pair<?>> attributes) {
        StringBuilder statement = new StringBuilder(" insert into " + sqlTableObject.getTableName() + " (");
        StringBuilder toConcat = new StringBuilder(") values (");
        for (int i = 0; i < attributes.size(); i++) {
            String key = attributes.get(i).k();
            if (i < attributes.size() - 1) {
                statement.append(key).append(", ");
                toConcat.append(" ? , ");
            } else {
                statement.append(key);
                toConcat.append(" ? ");
            }
        }
        statement.append(toConcat).append(")");
        return statement.toString();

    }

    public InsertCache clear() throws SQLException {
        for (PreparedStatement p : this.statementMap.values())
            p.close();
        this.statementMap = new HashMap<>();
        return this;
    }


    public Long insertWithAttributes(SQLTableObject sqlTableObject, List<Pair<?>> attributes) throws SqlQueriesException {
        if (attributes == null) {
            System.err.println("SQLQueries.insertWithAttributes: attributes are null.");
            System.err.println("SQLQueries.insertWithAttributes: have you called init() in the constructor of " + sqlTableObject.getClass().getSimpleName() + "?");
        }
        try {
            PreparedStatement preparedStatement = this.getInsertStatement(sqlTableObject, attributes);
            for (int i = 1; i <= attributes.size(); i++) {
                Pair<?> attribute = attributes.get(i - 1);
                preparedStatement.setObject(i, attribute.v());
            }
            this.debugTimer.start();
            preparedStatement.executeUpdate();
            this.debugTimer.stop();

            ResultSet resultSet = preparedStatement.getGeneratedKeys();
            resultSet.next();
            if (resultSet.getRow() > 0) {
                Object id = resultSet.getObject(1);
                resultSet.close();
                if (!this.active) {
                    preparedStatement.close();
                }
                if (id instanceof Integer)
                    return Long.valueOf((Integer) id);
                if (id instanceof Long)
                    return (Long) id;
            }
        } catch (Exception e) {
            System.err.println("SQLQueries.insert.excep: " + e.getClass().getSimpleName() + ", msg: " + e.getMessage());
            StringBuilder attrs = new StringBuilder();
            for (Pair<?> pair : attributes) {
                attrs.append(pair.v()).append(", ");
            }
            System.err.println("SQLQueries.insert.attributes: " + attrs.toString());

            throw new SqlQueriesException(e);
        }
        return null;
    }


}
