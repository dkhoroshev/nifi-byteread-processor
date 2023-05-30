package com.github.shivvers.nifi.service;

import com.github.shivvers.nifi.extension.MessageReadingException;
import com.github.shivvers.nifi.extension.UnknownMessageTypeException;
import com.github.shivvers.nifi.mapper.ByteIntMap;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ByteService {

    /*
     * Handle all the logic leading to split bynary given data
     * @param size How many bytes is the length of the message
     * @patam in The input stream, bynary data
     * @param out The stream where to output the byte array data
     * @throws IOException  Thrown when an errors occurs while parsing the data
     * @throws MessageEncodingException Thrown when an error occurs during the binary encoding
     * @throws UnknownMessageTypeException  Thrown when the given message type is not contained in the schema
     */
    public static void readMessage(Integer size, InputStream in, List<byte[]> messages) throws IOException, UnknownMessageTypeException, MessageReadingException {
        if (size == null) {
            throw new IOException("Message size address is null!");
        }

        final int MESSAGE_SIZE_HEADER_SIZE = size.intValue();
        final int messageLen = in.available();
        int bytesRead = 0;

        while (bytesRead <= messageLen){
            byte[] messageSizeHeader = new byte[MESSAGE_SIZE_HEADER_SIZE];
            int bytesReadInHeader = in.read(messageSizeHeader);
            if (bytesReadInHeader == -1) { break; }

            int messageSize = ByteIntMap.byteArrayToInt(messageSizeHeader);

            bytesRead += bytesReadInHeader;

            byte[] message = new byte[messageSize];
            int bytesReadInMessage = 0;
            while (bytesReadInMessage < messageSize && bytesReadInMessage != -1){
                bytesReadInMessage += in.read(message);
            }

            bytesRead += bytesReadInMessage;

            messages.add(message);

        }
    }
}
