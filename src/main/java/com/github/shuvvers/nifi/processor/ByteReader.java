package com.github.shuvvers.nifi.processor;

import com.github.shuvvers.nifi.extension.UnknownMessageTypeException;
import com.github.shuvvers.nifi.extension.MessageReadingException;
import com.github.shuvvers.nifi.service.ByteService;

import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import javax.management.Descriptor;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicReference;

@SideEffectFree
@Tags({"read", "binary", "message"})
@CapabilityDescription("Read data from the message according to its length specified before the message fragment in Bytes sequence.")
public class ByteReader extends ByteProcessor {

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {

        final AtomicReference<Relationship> error = new AtomicReference<>();

        final FlowFile flowFile = processSession.get();

        Integer bytesofsize = processContext.getProperty(BYTES_TO_READ.getName()).asInteger();

        if (bytesofsize == null){
            getLogger().error("Unable to find message light bytes in property");
            processSession.transfer(flowFile, ERROR);
        } else {

            FlowFile outputFlowFile = processSession.write(flowFile, (InputStream in, OutputStream out) -> {
               try {
                   // TODO: ByteService.readMessage(byteofsize, in, out);
               } catch (Exception e) {
                   getLogger().error(e.getMessage(), e);
                   error.set(ERROR);
               }
            });

            if (error.get() != null) {
                processSession.transfer(flowFile, error.get());
            } else {
                processSession.transfer(outputFlowFile, SUCCESS);
            }
        }
    }
}
