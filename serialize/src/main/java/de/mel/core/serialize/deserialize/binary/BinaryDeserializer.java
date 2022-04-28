package de.mel.core.serialize.deserialize.binary;

import de.mel.core.serialize.SerializableEntity;
import de.mel.core.serialize.deserialize.FieldDeserializer;
import de.mel.core.serialize.deserialize.entity.SerializableEntityDeserializer;
import de.mel.core.serialize.exceptions.JsonDeserializationException;

import java.lang.reflect.Field;
import java.util.Base64;

/**
 * Created by xor on 2/26/16.
 */
public class BinaryDeserializer implements FieldDeserializer {

    public interface Base64Decoder {
        byte[] decode(String string);
    }

    private static Base64Decoder base64Decoder = string -> Base64.getDecoder().decode(string);

    public static void setBase64Decoder(Base64Decoder base64Decoder) {
        BinaryDeserializer.base64Decoder = base64Decoder;
    }

    public static byte[] decode(String base64){
        return base64Decoder.decode(base64);
    }
    @Override
    public Object deserialize(SerializableEntityDeserializer serializableEntityDeserializer, SerializableEntity entity, Field field, Class typeClass, Object value) throws IllegalAccessException, JsonDeserializationException {
        //value is expected to be Base64 encoded
        Class<?> fieldClass = field.getType();
        if (value != null) {
            byte[] decoded = decode(value.toString());
            field.set(entity, decoded);
            return decoded;
        }
        return null;
    }
}
