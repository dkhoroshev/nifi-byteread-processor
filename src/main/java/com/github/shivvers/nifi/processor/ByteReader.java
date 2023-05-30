package com.github.shivvers.nifi.processor;

import com.github.shivvers.nifi.service.ByteService;

import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@SideEffectFree
@Tags({"read", "binary", "message"})
@CapabilityDescription("Read data from the message according to its length specified before the message fragment in Bytes sequence.")
public class ByteReader extends ByteProcessor {

    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();
    public static final String SEGMENT_ORIGINAL_FILENAME = FragmentAttributes.SEGMENT_ORIGINAL_FILENAME.key();
    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

        final AtomicReference<Relationship> error = new AtomicReference<>();

        final FlowFile flowFile = processSession.get();

        Integer bytesofsize = processContext.getProperty(BYTES_TO_READ.getName()).asInteger();

        if (bytesofsize == null){
            getLogger().error("Unable to find message light bytes in property");
            processSession.transfer(flowFile, ERROR);
        } else {
            List<byte[]> messages = new ArrayList<>();
//            final Map<String, String> originalAttributes = flowFile.getAttributes();
            final String fragmentId = UUID.randomUUID().toString();
            final List<FlowFile> flowFileArrayList = new ArrayList<>();
            try {
                processSession.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream in) throws IOException {
                        try {
                            ByteService.readMessage(bytesofsize, in, messages);
                        } catch (Exception e) {
                            getLogger().error(e.getMessage(), e);
                            error.set(ERROR);
                        }
                        for (int i = 0; i < messages.size(); i++) {
                            byte[] message = messages.get(i);
                            FlowFile outputFlowFile = processSession.create(flowFile);

                            try {
                                final Map<String, String> attributes = new HashMap<>();
                                try (final OutputStream out = processSession.write(outputFlowFile)) {
                                    out.write(message);
                                    attributes.put("record.count", String.valueOf(i));
                                    attributes.put(FRAGMENT_ID, fragmentId);
                                    attributes.put(FRAGMENT_INDEX, String.valueOf(i));
                                    processSession.adjustCounter("Records Split", i, false);
                                }
                                outputFlowFile = processSession.putAllAttributes(outputFlowFile, attributes);
                            } finally {
                                flowFileArrayList.add(outputFlowFile);
                            }
                        }
                    }
                });
            } catch (final ProcessException pe) {
                getLogger().error("Failed to split {}", new Object[] {flowFile, pe});
                processSession.remove(flowFileArrayList);
                processSession.transfer(flowFile, ERROR);
                return;
            }
            for (int i = 0; i < flowFileArrayList.size(); i++) {
                FlowFile flowFile1 = flowFileArrayList.get(i);
                processSession.putAttribute(flowFile1, FRAGMENT_COUNT, String.valueOf(flowFileArrayList.size()));
            }
            processSession.transfer(flowFileArrayList, REL_SPLITS);
            getLogger().info("Successfully split {} into {} FlowFiles", new Object[] {flowFile, flowFileArrayList.size()});
            processSession.remove(flowFile);
        }
    }
}
