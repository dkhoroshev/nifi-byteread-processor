package com.github.shuvvers.nifi.processor;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

public abstract class ByteProcessor extends AbstractProcessor {
    /**
     * NiFi properties of the processor, that can be configured using the Web UI
     */
    private List<PropertyDescriptor> properties;

    /**
     * The different relationships of the processor
     */
    private Set<Relationship> relationships;

    private Integer bytesofsize;

    static final PropertyDescriptor BYTES_TO_READ = new PropertyDescriptor.Builder()
            .name("read.bytes")
            .displayName("Read Bytes")
            .defaultValue("4")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .description("How many Bytes to read to get the length of the message." +
                    "2B - int16, 4B - int32, 8B - int64")
            .build();

    /*          RELATIONSHIPS           */

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Success relationship")
            .build();

    static final Relationship ERROR = new Relationship.Builder()
            .name("error")
            .description("Error relationship")
            .build();

    @Override
    public void init(final ProcessorInitializationContext context){
        this.properties = List.of(BYTES_TO_READ);

        this.relationships = Set.of(SUCCESS, ERROR);

        this.bytesofsize = 4;
    }

    /**
     * Compile the Bytes value
     *
     * @see AbstractProcessor
     */
    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);

        if (descriptor == BYTES_TO_READ) {
            this.bytesofsize = Integer.parseInt(newValue);
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
}
