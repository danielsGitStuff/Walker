package de.mel.core.sql.classes;

import de.mel.sql.Pair;
import de.mel.sql.SQLTableObject;

public class Door extends SQLTableObject {
    public static String ID = "id";
    public static String ID_HOUSE = "id_house";
    public static String HINGES = "hinges";

    private final Pair<Long> id = new Pair<>(Long.class, ID);
    private final Pair<Long> idHouse = new Pair<>(Long.class, ID_HOUSE);
    private final Pair<Integer> hinges = new Pair<>(Integer.class, HINGES);

    public Door() {
        init();
    }

    @Override
    public void onInsert(Long... ids) {
        this.id.v(ids[0]);
    }

    @Override
    public String getTableName() {
        return "door";
    }

    @Override
    protected void init() {
        populateInsert(idHouse, hinges);
        populateAll(id);
    }

    public Long getId() {
        return id.v();
    }

    public Pair<Long> getIdPair() {
        return id;
    }

    public Long getIdHouse() {
        return idHouse.v();
    }

    public Pair<Long> getIdHousePair() {
        return idHouse;
    }

    public Integer getHinges() {
        return hinges.v();
    }

    public Pair<Integer> getHingesPair() {
        return hinges;
    }


}
