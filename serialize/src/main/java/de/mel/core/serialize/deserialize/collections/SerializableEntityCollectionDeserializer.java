package de.mel.core.serialize.deserialize.collections;


import de.mel.core.serialize.SerializableEntity;
import de.mel.core.serialize.deserialize.FieldDeserializer;
import de.mel.core.serialize.deserialize.entity.SerializableEntityDeserializer;
import de.mel.core.serialize.exceptions.JsonDeserializationException;

import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.*;

/**
 * Created by xor on 12/23/15.
 */
public class SerializableEntityCollectionDeserializer implements FieldDeserializer {
    public SerializableEntityCollectionDeserializer(SerializableEntityDeserializer rootDeserializer, Field field) {

    }

    public static <T> Collection createCollection(Class<T> type) throws JsonDeserializationException {
        if (List.class.isAssignableFrom(type))
            return new ArrayList<T>();
        else if (Set.class.isAssignableFrom(type))
            return new HashSet<T>();
        throw new JsonDeserializationException("Could not instantiate a List or set given that Type: " + type);
    }


    @Override
    public Object deserialize(SerializableEntityDeserializer serializableEntityDeserializer, SerializableEntity entity, Field field, Class typeClass, Object jsonFieldValue) throws JsonDeserializationException, IllegalAccessException {
        if (jsonFieldValue != null) {
            // !!!!! stuff commented out below might be unnecessary !!!!!
            // check if entity or just a string
//            ParameterizedType genericListType = (ParameterizedType) field.getGenericType();
//            Class<?> genericListClass = (Class<?>) genericListType.getActualTypeArguments()[0];
            JSONArray jsonArray = (JSONArray) jsonFieldValue;
            int length = jsonArray.length();
//            if (SerializableEntity.class.isAssignableFrom(genericListClass)) {
                Collection entities = createCollection(field.getType());
                for (int i = 0; i < length; i++) {
                    Object something = jsonArray.get(i);
                    JSONObject jsonObject = null;
                    if (!(something instanceof JSONObject.Null)) {
                        jsonObject = jsonArray.getJSONObject(i);
                    }
                    SerializableEntity arrEntity = serializableEntityDeserializer.buildEntity(jsonObject);
                    entities.add(arrEntity);
                }
                field.set(entity, entities);
                return entities;
//            }
        }
        return null;
    }
}
