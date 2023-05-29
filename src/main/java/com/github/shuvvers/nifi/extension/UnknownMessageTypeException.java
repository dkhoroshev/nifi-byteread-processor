package com.github.shuvvers.nifi.extension;

public class UnknownMessageTypeException extends Exception {
    public UnknownMessageTypeException(String messageType) {
        super("No message type '" + messageType + "' found in the schema file.");
    }
}
