package de.mel.sql;

/**
 * Created by xor on 25.10.2015.
 */
public abstract class Dao {
    protected ASQLQueries sqlQueries;
    protected final boolean lock;

    public Dao(ASQLQueries sqlQueries) {
        this(sqlQueries, true);
    }

    public Dao(ASQLQueries sqlQueries, boolean lock) {
        this.sqlQueries = sqlQueries;
        this.lock = lock;
    }

    public ASQLQueries getSqlQueries() {
        return sqlQueries;
    }

    /**
     * Created by xor on 11/25/16.
     */
    public static class LockingDao extends ConnectionLockingDao {


        public LockingDao(ASQLQueries ASQLQueries) {
            super(ASQLQueries);
        }

        public LockingDao(ASQLQueries ASQLQueries, boolean lock) {
            super(ASQLQueries, lock);
        }
    }

    /**
     * Created by xor on 11/25/16.
     */
    public abstract static class ConnectionLockingDao extends Dao {
        public ConnectionLockingDao(ASQLQueries ASQLQueries) {
            super(ASQLQueries);
        }

        public ConnectionLockingDao(ASQLQueries ASQLQueries, boolean lock) {
            super(ASQLQueries, lock);
        }

    }
}
