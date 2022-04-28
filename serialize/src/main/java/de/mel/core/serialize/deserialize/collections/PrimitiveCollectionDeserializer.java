package de.mel.core.serialize.deserialize.collections;

import de.mel.core.serialize.SerializableEntity;
import de.mel.core.serialize.deserialize.FieldDeserializer;
import de.mel.core.serialize.deserialize.entity.SerializableEntityDeserializer;
import de.mel.core.serialize.deserialize.primitive.PrimitiveDeserializer;
import de.mel.core.serialize.exceptions.JsonDeserializationException;
import org.json.JSONArray;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;

/**
 * Created by xor on 12/12/16.
 */
public class PrimitiveCollectionDeserializer implements FieldDeserializer {
    @Override
    public Object deserialize(SerializableEntityDeserializer serializableEntityDeserializer, SerializableEntity entity, Field field, Class typeClass, Object jsonFieldValue) throws IllegalAccessException, JsonDeserializationException {
        JSONArray jsonArray = (JSONArray) jsonFieldValue;
        if (jsonFieldValue != null) {
            ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
            Object whatever = parameterizedType.getActualTypeArguments()[0];

            Collection collection = SerializableEntityCollectionDeserializer.createCollection(field.getType());

            Class<?> genericType = (Class<?>) whatever;
            for (int i = 0; i < jsonArray.length(); i++) {
                Object value = PrimitiveDeserializer.JSON2Primtive(genericType, ((JSONArray) jsonFieldValue).get(i));
                collection.add(value);
            }
            field.set(entity, collection);
            return collection;
        }
        return null;
    }
}
