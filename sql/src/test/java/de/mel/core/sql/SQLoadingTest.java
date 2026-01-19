package de.mel.core.sql;

import de.mel.core.sql.classes.Door;
import de.mel.core.sql.classes.House;
import de.mel.execute.SqliteExecutor;
import de.mel.sql.ASQLQueries;
import de.mel.sql.SQLQueries;
import de.mel.sql.SqlQueriesException;
import de.mel.sql.SqliteQueriesCreator;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.*;

public class SQLoadingTest {
    public SQLQueries sqlQueries;

    @Before
    public void before() {
        sqlQueries = SqliteQueriesCreator.createSqliteQueries(new File("test.sqlite.db"));
        try (InputStream stream = this.getClass().getResourceAsStream("/loadingtest.sql")) {
            new SqliteExecutor(sqlQueries.getSQLConnection()).executeStream(stream);
            House h1 = new House();
            h1.getNamePair().v("test name");
            House h2 = new House();
            h2.getNamePair().v("test name 22");
            Door d1 = new Door();
            d1.getHingesPair().v(2);
            d1.getIdHousePair().v(h1.getIdPair());
            Door d2 = new Door();
            d2.getHingesPair().v(3);
            d2.getIdHousePair().v(h1.getIdPair());
            Door d3 = new Door();
            d3.getHingesPair().v(4);
            d3.getIdHousePair().v(h2.getIdPair());
            sqlQueries.insert(h1);
            sqlQueries.insert(h2);
            sqlQueries.insert(d1);
            sqlQueries.insert(d2);
            sqlQueries.insert(d3);
        } catch (IOException | SQLException | SqlQueriesException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test() throws SqlQueriesException {
        List<Door> doors = sqlQueries.load(new Door());
        assertEquals(3, doors.size());
        House h1 = sqlQueries.loadFirstRow(new House(), House.ID + "=?", ASQLQueries.args(1L), House.class);
        List<Door> doors1 = sqlQueries.load(new Door(), Door.ID_HOUSE + "=?", ASQLQueries.args(h1.getId()));
        assertEquals(2, doors1.size());
    }
}
