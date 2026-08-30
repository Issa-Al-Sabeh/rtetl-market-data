package com.issaalsabeh.etl.config;

import java.util.List;
import java.util.Map;

public class PipelineConfig {

    private PipelineDefinition pipeline;

    public PipelineDefinition getPipeline() {
        return pipeline;
    }

    public void setPipeline(PipelineDefinition pipeline) {
        this.pipeline = pipeline;
    }

    public static class PipelineDefinition {

        private ConnectorConfig source;
        private List<String> transformations;
        private List<ConnectorConfig> sinks;
        private ConnectorConfig deadLetterQueue;

        public ConnectorConfig getSource() {
            return source;
        }

        public void setSource(ConnectorConfig source) {
            this.source = source;
        }

        public List<String> getTransformations() {
            return transformations;
        }

        public void setTransformations(List<String> transformations) {
            this.transformations = transformations;
        }

        public List<ConnectorConfig> getSinks() {
            return sinks;
        }

        public void setSinks(List<ConnectorConfig> sinks) {
            this.sinks = sinks;
        }

        public ConnectorConfig getDeadLetterQueue() {return deadLetterQueue;}

        public void setDeadLetterQueue(ConnectorConfig deadLetterQueue) {this.deadLetterQueue = deadLetterQueue;}
    }

    public static class ConnectorConfig {

        private String type;
        private Map<String, String> properties;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Map<String, String> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, String> properties) {
            this.properties = properties;
        }
    }
}