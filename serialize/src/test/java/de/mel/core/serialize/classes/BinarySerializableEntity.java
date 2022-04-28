package de.mel.core.serialize.classes;

import de.mel.core.serialize.SerializableEntity;

/**
 * Created by xor on 2/26/16.
 */
public class BinarySerializableEntity implements SerializableEntity {
    private byte[] binary;

    public BinarySerializableEntity setBinary(byte[] binary) {
        this.binary = binary;
        return this;
    }

    public byte[] getBinary() {
        return binary;
    }
}
