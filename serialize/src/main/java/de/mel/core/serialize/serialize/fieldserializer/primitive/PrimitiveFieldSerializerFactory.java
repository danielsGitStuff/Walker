package de.mel.core.serialize.serialize.fieldserializer.primitive;

import de.mel.core.serialize.serialize.fieldserializer.FieldSerializer;
import de.mel.core.serialize.serialize.fieldserializer.FieldSerializerFactory;
import de.mel.core.serialize.serialize.fieldserializer.NullSerializer;
import de.mel.core.serialize.serialize.fieldserializer.entity.SerializableEntitySerializer;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by xor on 12/20/15.
 */
public class PrimitiveFieldSerializerFactory implements FieldSerializerFactory {
    private static PrimitiveFieldSerializerFactory ins;

    public PrimitiveFieldSerializerFactory() {
        for (Class clazz : getClasses())
            primitiveClasses.add(clazz);
        //Arrays.stream(getClasses()).forEach(primitiveClasses::add);
    }

    public static PrimitiveFieldSerializerFactory getInstance() {
        if (ins == null)
            ins = new PrimitiveFieldSerializerFactory();
        return ins;
    }

    private final Set<Class<?>> primitiveClasses = new HashSet();

    public static Class<?>[] getClasses() {
        return new Class<?>[]{Byte.class, byte.class, short.class, Short.class, int.class, Integer.class,
                long.class, Long.class, float.class, Float.class, double.class, Double.class, char.class,
                Character.class, String.class, boolean.class, Boolean.class};
    }

    @Override
    public FieldSerializer createSerializer(SerializableEntitySerializer parentSerializer, Field field) throws IllegalAccessException {
        field.setAccessible(true);
        Object whatever = field.get(parentSerializer.getEntity());
        if (whatever == null)
            return new NullSerializer();
        return new PrimitiveFieldSerializer(whatever);
    }

    @Override
    public boolean canSerialize(Field field) {
        return primitiveClasses.contains(field.getType());
    }

    @Override
    public FieldSerializer createSerializerOnClass(SerializableEntitySerializer parentSerializer, Object value) {
        return new PrimitiveFieldSerializer(value);
    }

}
