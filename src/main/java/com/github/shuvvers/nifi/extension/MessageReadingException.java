package com.github.shuvvers.nifi.extension;

import java.io.IOException;

public class MessageReadingException extends Exception {
    public MessageReadingException(IOException parent) {
        super("Unable to read data: " + parent.getMessage(), parent);
    }
}
