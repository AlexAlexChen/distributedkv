package com.distributedkv.common.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Utility class for byte array operations
 */
public class ByteUtil {
    /**
     * Convert a long to a byte array
     *
     * @param value the long value
     * @return the byte array
     */
    public static byte[] longToBytes(long value) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte) (value & 0xFF);
            value >>= 8;
        }
        return result;
    }

    /**
     * Convert a byte array to a long
     *
     * @param bytes the byte array
     * @return the long value
     * @throws IllegalArgumentException if the byte array is too short
     */
    public static long bytesToLong(byte[] bytes) {
        if (bytes.length < 8) {
            throw new IllegalArgumentException("Byte array too short for long conversion");
        }

        long result = 0;
        for (int i = 0; i < 8; i++) {
            result <<= 8;
            result |= (bytes[i] & 0xFF);
        }
        return result;
    }

    /**
     * Convert an int to a byte array
     *
     * @param value the int value
     * @return the byte array
     */
    public static byte[] intToBytes(int value) {
        byte[] result = new byte[4];
        for (int i = 3; i >= 0; i--) {
            result[i] = (byte) (value & 0xFF);
            value >>= 8;
        }
        return result;
    }

    /**
     * Convert a byte array to an int
     *
     * @param bytes the byte array
     * @return the int value
     * @throws IllegalArgumentException if the byte array is too short
     */
    public static int bytesToInt(byte[] bytes) {
        if (bytes.length < 4) {
            throw new IllegalArgumentException("Byte array too short for int conversion");
        }

        int result = 0;
        for (int i = 0; i < 4; i++) {
            result <<= 8;
            result |= (bytes[i] & 0xFF);
        }
        return result;
    }

    /**
     * Convert a double to a byte array
     *
     * @param value the double value
     * @return the byte array
     */
    public static byte[] doubleToBytes(double value) {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).putDouble(value);
        return bytes;
    }

    /**
     * Convert a byte array to a double
     *
     * @param bytes the byte array
     * @return the double value
     * @throws IllegalArgumentException if the byte array is too short
     */
    public static double bytesToDouble(byte[] bytes) {
        if (bytes.length < 8) {
            throw new IllegalArgumentException("Byte array too short for double conversion");
        }

        return ByteBuffer.wrap(bytes).getDouble();
    }

    /**
     * Convert a string to a byte array using UTF-8 encoding
     *
     * @param value the string value
     * @return the byte array
     */
    public static byte[] stringToBytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Convert a byte array to a string using UTF-8 encoding
     *
     * @param bytes the byte array
     * @return the string value
     */
    public static String bytesToString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Compare two byte arrays lexicographically
     *
     * @param bytes1 first byte array
     * @param bytes2 second byte array
     * @return negative if bytes1 &lt; bytes2, 0 if bytes1 == bytes2, positive if bytes1 &gt; bytes2
     */
    public static int compare(byte[] bytes1, byte[] bytes2) {
        int minLength = Math.min(bytes1.length, bytes2.length);

        for (int i = 0; i < minLength; i++) {
            int v1 = bytes1[i] & 0xFF;
            int v2 = bytes2[i] & 0xFF;

            if (v1 != v2) {
                return v1 - v2;
            }
        }

        return bytes1.length - bytes2.length;
    }

    /**
     * Concatenate multiple byte arrays
     *
     * @param arrays the byte arrays to concatenate
     * @return the concatenated byte array
     */
    public static byte[] concat(byte[]... arrays) {
        int length = 0;
        for (byte[] array : arrays) {
            length += array.length;
        }

        byte[] result = new byte[length];
        int pos = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, result, pos, array.length);
            pos += array.length;
        }

        return result;
    }

    /**
     * Extract a sub-array from a byte array
     *
     * @param bytes the source byte array
     * @param offset the start offset
     * @param length the length to extract
     * @return the sub-array
     * @throws IllegalArgumentException if offset or length are invalid
     */
    public static byte[] subArray(byte[] bytes, int offset, int length) {
        if (offset < 0 || length < 0 || offset + length > bytes.length) {
            throw new IllegalArgumentException("Invalid offset or length");
        }

        byte[] result = new byte[length];
        System.arraycopy(bytes, offset, result, 0, length);
        return result;
    }

    /**
     * Check if a byte array starts with a prefix
     *
     * @param bytes the byte array
     * @param prefix the prefix to check
     * @return true if the byte array starts with the prefix
     */
    public static boolean startsWith(byte[] bytes, byte[] prefix) {
        if (bytes.length < prefix.length) {
            return false;
        }

        for (int i = 0; i < prefix.length; i++) {
            if (bytes[i] != prefix[i]) {
                return false;
            }
        }

        return true;
    }

    /**
     * Check if a byte array ends with a suffix
     *
     * @param bytes the byte array
     * @param suffix the suffix to check
     * @return true if the byte array ends with the suffix
     */
    public static boolean endsWith(byte[] bytes, byte[] suffix) {
        if (bytes.length < suffix.length) {
            return false;
        }

        int offset = bytes.length - suffix.length;
        for (int i = 0; i < suffix.length; i++) {
            if (bytes[offset + i] != suffix[i]) {
                return false;
            }
        }

        return true;
    }

    /**
     * Check if a byte array contains a specific byte sequence
     *
     * @param bytes the byte array
     * @param sequence the sequence to find
     * @return true if the byte array contains the sequence
     */
    public static boolean contains(byte[] bytes, byte[] sequence) {
        if (sequence.length == 0) {
            return true;
        }
        if (bytes.length < sequence.length) {
            return false;
        }

        for (int i = 0; i <= bytes.length - sequence.length; i++) {
            boolean found = true;
            for (int j = 0; j < sequence.length; j++) {
                if (bytes[i + j] != sequence[j]) {
                    found = false;
                    break;
                }
            }
            if (found) {
                return true;
            }
        }

        return false;
    }

    /**
     * Convert a hex string to a byte array
     *
     * @param hex the hex string
     * @return the byte array
     * @throws IllegalArgumentException if the hex string is invalid
     */
    public static byte[] hexToBytes(String hex) {
        if (hex.length() % 2 != 0) {
            throw new IllegalArgumentException("Hex string must have an even length");
        }

        byte[] result = new byte[hex.length() / 2];
        for (int i = 0; i < result.length; i++) {
            int high = Character.digit(hex.charAt(i * 2), 16);
            int low = Character.digit(hex.charAt(i * 2 + 1), 16);
            if (high == -1 || low == -1) {
                throw new IllegalArgumentException("Invalid hex string");
            }
            result[i] = (byte) ((high << 4) | low);
        }

        return result;
    }

    /**
     * Convert a byte array to a hex string
     *
     * @param bytes the byte array
     * @return the hex string
     */
    public static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xFF));
        }
        return sb.toString();
    }

    /**
     * Create a copy of a byte array
     *
     * @param bytes the byte array to copy
     * @return a new copy of the byte array
     */
    public static byte[] copy(byte[] bytes) {
        return Arrays.copyOf(bytes, bytes.length);
    }
}
