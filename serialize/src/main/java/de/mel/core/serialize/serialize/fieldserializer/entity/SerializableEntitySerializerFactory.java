package de.mel.core.serialize.serialize.fieldserializer.entity;

import de.mel.core.serialize.SerializableEntity;
import de.mel.core.serialize.exceptions.JsonSerializationException;
import de.mel.core.serialize.serialize.fieldserializer.FieldSerializer;
import de.mel.core.serialize.serialize.fieldserializer.FieldSerializerFactory;
import de.mel.core.serialize.serialize.fieldserializer.NullSerializer;

import java.lang.reflect.Field;

/**
 * Created by xor on 12/20/15.
 */
public class SerializableEntitySerializerFactory implements FieldSerializerFactory {
    private static SerializableEntitySerializerFactory ins;

    public SerializableEntitySerializerFactory() {

    }

    public static SerializableEntitySerializerFactory getInstance() {
        if (ins == null)
            ins = new SerializableEntitySerializerFactory();
        return ins;
    }

    @Override
    public FieldSerializer createSerializer(SerializableEntitySerializer parentSerializer, Field field) throws IllegalAccessException, JsonSerializationException {
        field.setAccessible(true);
        SerializableEntity entity = (SerializableEntity) field.get(parentSerializer.getEntity());
        if (entity == null)
            return new NullSerializer();
        return parentSerializer.getPreparedSerializer(entity);
    }

    @Override
    public boolean canSerialize(Field field) {
        return SerializableEntity.class.isAssignableFrom(field.getType());
    }

    @Override
    public FieldSerializer createSerializerOnClass(SerializableEntitySerializer parentSerializer, Object value) {
        return new SerializableEntitySerializer( parentSerializer, (SerializableEntity) value);
    }

}
