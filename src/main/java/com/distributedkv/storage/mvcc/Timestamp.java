package com.distributedkv.storage.mvcc;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a logical timestamp for MVCC operations.
 *
 * Timestamps are used in MVCC to order transactions and determine visibility
 * of different versions of data. Each transaction has a start timestamp and a
 * commit timestamp.
 */
public class Timestamp implements Comparable<Timestamp>, Serializable {
    private static final long serialVersionUID = 1L;

    /** Global timestamp allocator */
    private static final AtomicLong TIMESTAMP_ALLOCATOR = new AtomicLong(1);

    /** Special sentinel timestamp representing the oldest possible timestamp */
    public static final Timestamp MIN_TIMESTAMP = new Timestamp(0);

    /** Special sentinel timestamp representing the newest possible timestamp */
    public static final Timestamp MAX_TIMESTAMP = new Timestamp(Long.MAX_VALUE);

    /** The timestamp value */
    private final long value;

    /**
     * Creates a new timestamp with the specified value.
     *
     * @param value The timestamp value
     */
    private Timestamp(long value) {
        this.value = value;
    }

    /**
     * Allocates a new unique timestamp.
     *
     * @return A new timestamp
     */
    public static Timestamp allocate() {
        return new Timestamp(TIMESTAMP_ALLOCATOR.getAndIncrement());
    }

    /**
     * Creates a timestamp with the specified value.
     *
     * @param value The timestamp value
     * @return A timestamp with the specified value
     */
    public static Timestamp of(long value) {
        if (value == 0) {
            return MIN_TIMESTAMP;
        } else if (value == Long.MAX_VALUE) {
            return MAX_TIMESTAMP;
        } else {
            return new Timestamp(value);
        }
    }

    /**
     * Gets the value of this timestamp.
     *
     * @return The timestamp value
     */
    public long getValue() {
        return value;
    }

    /**
     * Checks if this timestamp is after another timestamp.
     *
     * @param other The other timestamp
     * @return True if this timestamp is after the other timestamp
     */
    public boolean isAfter(Timestamp other) {
        return this.value > other.value;
    }

    /**
     * Checks if this timestamp is before another timestamp.
     *
     * @param other The other timestamp
     * @return True if this timestamp is before the other timestamp
     */
    public boolean isBefore(Timestamp other) {
        return this.value < other.value;
    }

    /**
     * Checks if this timestamp is between two other timestamps (inclusive).
     *
     * @param lower The lower timestamp
     * @param upper The upper timestamp
     * @return True if this timestamp is between the lower and upper timestamps
     */
    public boolean isBetween(Timestamp lower, Timestamp upper) {
        return this.value >= lower.value && this.value <= upper.value;
    }

    @Override
    public int compareTo(Timestamp other) {
        return Long.compare(this.value, other.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Timestamp other = (Timestamp) obj;
        return this.value == other.value;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }

    @Override
    public String toString() {
        return Long.toString(value);
    }
}
