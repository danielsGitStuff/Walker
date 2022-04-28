package de.mel.sql.deserialize;

import org.json.JSONObject;

import de.mel.core.serialize.SerializableEntity;
import de.mel.core.serialize.deserialize.binary.BinaryDeserializer;
import de.mel.core.serialize.deserialize.FieldDeserializer;
import de.mel.core.serialize.deserialize.entity.SerializableEntityDeserializer;
import de.mel.core.serialize.exceptions.JsonDeserializationException;
import de.mel.sql.Pair;

import java.lang.reflect.Field;

/**
 * Created by xor on 1/14/16.
 */
public class PairDeserializer implements FieldDeserializer {
    public PairDeserializer(SerializableEntityDeserializer rootDeserializer, Field field) {

    }

    public static void deserialize(Pair pair, JSONObject jsonObject){

    }

    @Override
    public Object deserialize(SerializableEntityDeserializer serializableEntityDeserializer, SerializableEntity entity, Field field, Class typeClass, Object jsonFieldValue) throws IllegalAccessException, JsonDeserializationException {
        field.setAccessible(true);
        Pair<?> pair = (Pair<?>) field.get(entity);
        Object valueToSet = null;
        if (pair.getGenericClass().equals(byte[].class) && jsonFieldValue != null) {
            valueToSet = BinaryDeserializer.decode(jsonFieldValue.toString());
        } else {
            valueToSet = jsonFieldValue;
        }
        pair.setValueUnsecure(valueToSet);
        return pair;
    }
}
