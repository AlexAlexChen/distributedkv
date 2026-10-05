package com.distributedkv.storage.engines.hashtable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Wrapper for byte arrays to use them as keys in hash maps.
 *
 * This class provides proper equals and hashCode implementations for byte arrays,
 * allowing them to be used as keys in hash maps. It also implements Comparable
 * to allow for ordering byte arrays when needed.
 */
public class ByteArrayWrapper implements Comparable<ByteArrayWrapper>, Serializable {
    private static final long serialVersionUID = 1L;

    /** The wrapped byte array */
    private final byte[] data;

    /** The pre-computed hash code */
    private final int hashCode;

    /**
     * Creates a new byte array wrapper.
     *
     * @param data The byte array to wrap
     */
    public ByteArrayWrapper(byte[] data) {
        this.data = data != null ? Arrays.copyOf(data, data.length) : null;
        this.hashCode = Arrays.hashCode(this.data);
    }

    /**
     * Gets the wrapped byte array.
     *
     * @return The byte array
     */
    public byte[] getData() {
        return data != null ? Arrays.copyOf(data, data.length) : null;
    }

    /**
     * Compares two byte arrays lexicographically.
     *
     * @param b1 The first byte array
     * @param b2 The second byte array
     * @return A negative, zero, or positive integer as the first array is less than,
     *         equal to, or greater than the second array
     */
    public static int compare(byte[] b1, byte[] b2) {
        if (b1 == b2) {
            return 0;
        }
        if (b1 == null) {
            return -1;
        }
        if (b2 == null) {
            return 1;
        }

        int len1 = b1.length;
        int len2 = b2.length;
        int lim = Math.min(len1, len2);

        for (int i = 0; i < lim; i++) {
            int a = b1[i] & 0xff;
            int b = b2[i] & 0xff;
            if (a != b) {
                return a - b;
            }
        }

        return len1 - len2;
    }

    @Override
    public int compareTo(ByteArrayWrapper other) {
        return compare(this.data, other.data);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ByteArrayWrapper other = (ByteArrayWrapper) obj;
        return Arrays.equals(data, other.data);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        if (data == null) {
            return "null";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("ByteArray[").append(data.length).append("]{");

        // Display up to 8 bytes as hex
        int displayLength = Math.min(data.length, 8);
        for (int i = 0; i < displayLength; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(String.format("%02x", data[i] & 0xff));
        }

        if (data.length > 8) {
            sb.append(", ...");
        }

        sb.append('}');
        return sb.toString();
    }
}
