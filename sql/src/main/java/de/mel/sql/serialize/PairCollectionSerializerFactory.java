package de.mel.sql.serialize;

import java.lang.reflect.Field;
import java.util.Collection;

import de.mel.core.serialize.exceptions.JsonSerializationException;
import de.mel.core.serialize.serialize.fieldserializer.FieldSerializer;
import de.mel.core.serialize.serialize.fieldserializer.FieldSerializerFactory;
import de.mel.core.serialize.serialize.fieldserializer.entity.SerializableEntitySerializer;
import de.mel.core.serialize.serialize.reflection.FieldAnalyzer;
import de.mel.sql.Pair;

/**
 * Created by xor on 10/11/17.
 */

public class PairCollectionSerializerFactory implements FieldSerializerFactory {
    private static PairCollectionSerializerFactory ins;

    public static PairCollectionSerializerFactory getInstance() {
        if (ins == null)
            ins = new PairCollectionSerializerFactory();
        return ins;
    }

    @Override
    public FieldSerializer createSerializer(SerializableEntitySerializer parentSerializer, Field field) throws IllegalAccessException, JsonSerializationException {
        field.setAccessible(true);
        Collection<Pair> pairs = (Collection<Pair>) field.get(parentSerializer.getEntity());
        return new PairCollectionSerializer(pairs);
    }

    @Override
    public boolean canSerialize(Field field) {
        return FieldAnalyzer.isGenericCollectionOfClass(field, Pair.class);
    }

    @Override
    public FieldSerializer createSerializerOnClass(SerializableEntitySerializer parentSerializer, Object value) {
        System.out.println("PairCollectionSerializerFactory.createSerializerOnClass");
        return null;
    }
}
