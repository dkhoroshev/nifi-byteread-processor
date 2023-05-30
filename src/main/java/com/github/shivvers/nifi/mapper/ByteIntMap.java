package com.github.shivvers.nifi.mapper;

public class ByteIntMap {
    public static int byteArrayToInt(byte[] bytes) {
        return (bytes[0] & 0xFF) |
                ((bytes[1] & 0xFF) << 8) |
                ((bytes[2] & 0xFF) << 16) |
                ((bytes[3] & 0xFF) << 24);
    }
}
