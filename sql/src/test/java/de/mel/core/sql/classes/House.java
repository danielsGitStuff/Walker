package de.mel.core.sql.classes;

import de.mel.sql.Pair;
import de.mel.sql.SQLTableObject;

public class House extends SQLTableObject {
    public static String ID = "id";
    public static String NAME = "name";

    private final Pair<Long> id = new Pair<>(Long.class, ID);
    private final Pair<String> name = new Pair<>(String.class, NAME);

    public House() {
        init();
    }

    @Override
    public void onInsert(Long... ids) {
        this.id.v(ids[0]);
    }

    @Override
    public String getTableName() {
        return "house";
    }

    @Override
    protected void init() {
        populateInsert(name);
        populateAll(id);
    }

    public Long getId() {
        return id.v();
    }

    public Pair<Long> getIdPair() {
        return id;
    }

    public String getName() {
        return name.v();
    }

    public Pair<String> getNamePair() {
        return name;
    }
}
