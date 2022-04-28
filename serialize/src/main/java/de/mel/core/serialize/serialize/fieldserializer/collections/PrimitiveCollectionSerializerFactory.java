package de.mel.core.serialize.serialize.fieldserializer.collections;

import de.mel.core.serialize.exceptions.JsonSerializationException;
import de.mel.core.serialize.serialize.fieldserializer.FieldSerializer;
import de.mel.core.serialize.serialize.fieldserializer.FieldSerializerFactory;
import de.mel.core.serialize.serialize.fieldserializer.NullSerializer;
import de.mel.core.serialize.serialize.fieldserializer.entity.SerializableEntitySerializer;
import de.mel.core.serialize.serialize.reflection.FieldAnalyzer;

import java.lang.reflect.Field;

/**
 * Created by xor on 12/12/16.
 */
public class PrimitiveCollectionSerializerFactory implements FieldSerializerFactory {
    private static PrimitiveCollectionSerializerFactory ins;

    @Override
    public FieldSerializer createSerializer(SerializableEntitySerializer parentSerializer, Field field) throws IllegalAccessException, JsonSerializationException {
        field.setAccessible(true);
        Iterable iterable = (Iterable) field.get(parentSerializer.getEntity());
        if (iterable == null)
            return new NullSerializer();
        return new PrimitiveCollectionSerializer(iterable);
    }

    @Override
    public boolean canSerialize(Field field) {
        return FieldAnalyzer.isPrimitiveCollection(field);
    }

    @Override
    public FieldSerializer createSerializerOnClass(SerializableEntitySerializer parentSerializer, Object value) {
        return null;
    }

    public static PrimitiveCollectionSerializerFactory getInstance() {
        if (ins == null)
            ins = new PrimitiveCollectionSerializerFactory();
        return ins;
    }
}
