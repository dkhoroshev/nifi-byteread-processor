package com.github.shivvers.nifi.processor;

import com.github.shivvers.nifi.extension.MessageReadingException;
import com.github.shivvers.nifi.extension.UnknownMessageTypeException;
import com.github.shivvers.nifi.service.ByteService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;

public class ByteReaderTest {

    /*
     * Test
     */
    @Test
    public void readFile() throws IOException, MessageReadingException, UnknownMessageTypeException {
//        TestRunner runner = TestRunners.newTestRunner(new ByteReader());
//        runner.setProperty(ByteProcessor.BYTES_TO_READ, "4");

        InputStream in = getClass().getResourceAsStream("/data/verve.binproto");
        PrintStream out = new PrintStream("target/byteservice.out");

        ByteService byteClass = new ByteService();
        byteClass.readMessage(4, in, out);
//        runner.enqueue(in);
//
//        runner.assertValid();
//        runner.run(1);
//        runner.assertQueueEmpty();
//
//        runner.assertAllFlowFilesTransferred(ByteReader.SUCCESS);
//        List<MockFlowFile> results = runner.getFlowFilesForRelationship(ByteReader.SUCCESS);
//        byte[] out = runner.getContentAsByteArray(ByteReader.SUCCESS);
//        f.write(out);

    }
}
