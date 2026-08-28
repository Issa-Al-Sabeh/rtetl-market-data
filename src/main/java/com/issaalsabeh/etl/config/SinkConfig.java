package com.issaalsabeh.etl.config;

import java.util.HashMap;
import java.util.Map;

public class SinkConfig {
    private final String type;
    private final Map<String, String> properties;

    public  SinkConfig(String type, Map<String, String> properties){
        if(type == null || type.isBlank()){
            throw new IllegalArgumentException("Type cannot be empty or null");
        }

        this.type = type;
        this.properties = properties == null
                ? new HashMap<>()
                : new HashMap<>(properties);
    }
    public String getType() {
        return type;
    }

    public String getProperty(String key) {

        String value = properties.get(key);
        if (value == null || value.isBlank()){
            throw new IllegalArgumentException("Missing required sink property: " + key);
        }
        return value;
    }
}
