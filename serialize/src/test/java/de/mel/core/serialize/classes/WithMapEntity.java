package de.mel.core.serialize.classes;

import de.mel.core.serialize.SerializableEntity;

import java.util.HashMap;
import java.util.Map;

public class WithMapEntity implements SerializableEntity {
    public Map<String,SimplestEntity> entities = new HashMap<>();
}
