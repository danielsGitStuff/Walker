package de.mel.core.serialize.deserialize.map;

import de.mel.core.serialize.deserialize.FieldDeserializer;
import de.mel.core.serialize.deserialize.FieldDeserializerFactory;
import de.mel.core.serialize.deserialize.entity.SerializableEntityDeserializer;
import de.mel.core.serialize.deserialize.entity.SerializableEntityDeserializerFactory;
import de.mel.core.serialize.serialize.reflection.FieldAnalyzer;

import java.lang.reflect.Field;

/**
 * Created by xor on 1/13/17.
 */
public class MapDeserializerFactory implements FieldDeserializerFactory {
    private static MapDeserializerFactory ins;

    public static MapDeserializerFactory getInstance() {
        if (ins == null)
            ins = new MapDeserializerFactory();
        return ins;
    }

    @Override
    public boolean canDeserialize(Field field) {
        return FieldAnalyzer.isMap(field);
    }

    @Override
    public FieldDeserializer createDeserializer(SerializableEntityDeserializer rootDeserializer, Field field) {
        MapDeserializer deserializer = new MapDeserializer(rootDeserializer, field);
        return deserializer;
    }
}
