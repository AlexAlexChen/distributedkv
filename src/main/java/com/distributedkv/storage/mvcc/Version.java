package com.distributedkv.storage.mvcc;

import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a versioned value in the MVCC system.
 *
 * Each key in the storage engine can have multiple versions, each with a
 * different timestamp range. The Version class encapsulates a specific version
 * of a value along with its validity timestamps.
 */
public class Version {
    /** The value */
    private final byte[] value;

    /** The timestamp when this version became valid */
    private final Timestamp startTimestamp;

    /** The timestamp when this version was created (by a transaction) */
    private final Timestamp createdAt;

    /** The timestamp when this version was committed */
    private volatile Timestamp commitTimestamp;

    /** The timestamp when this version was deleted (or null if not deleted) */
    private volatile Timestamp deletedAt;

    /**
     * Creates a new version.
     *
     * @param value The value
     * @param startTimestamp The timestamp when this version became valid
     * @param createdAt The timestamp when this version was created
     */
    public Version(byte[] value, Timestamp startTimestamp, Timestamp createdAt) {
        this.value = value != null ? Arrays.copyOf(value, value.length) : null;
        this.startTimestamp = startTimestamp;
        this.createdAt = createdAt;
        this.commitTimestamp = null;
        this.deletedAt = null;
    }

    /**
     * Gets the value.
     *
     * @return The value
     */
    public byte[] getValue() {
        return value != null ? Arrays.copyOf(value, value.length) : null;
    }

    /**
     * Gets the timestamp when this version became valid.
     *
     * @return The start timestamp
     */
    public Timestamp getStartTimestamp() {
        return startTimestamp;
    }

    /**
     * Gets the timestamp when this version was created.
     *
     * @return The created timestamp
     */
    public Timestamp getCreatedAt() {
        return createdAt;
    }

    /**
     * Gets the timestamp when this version was committed.
     *
     * @return The commit timestamp, or null if not committed
     */
    public Timestamp getCommitTimestamp() {
        return commitTimestamp;
    }

    /**
     * Sets the timestamp when this version was committed.
     *
     * @param commitTimestamp The commit timestamp
     */
    public void setCommitTimestamp(Timestamp commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    /**
     * Gets the timestamp when this version was deleted.
     *
     * @return The deleted timestamp, or null if not deleted
     */
    public Timestamp getDeletedAt() {
        return deletedAt;
    }

    /**
     * Sets the timestamp when this version was deleted.
     *
     * @param deletedAt The deleted timestamp
     */
    public void setDeletedAt(Timestamp deletedAt) {
        this.deletedAt = deletedAt;
    }

    /**
     * Checks if this version is committed.
     *
     * @return True if this version is committed
     */
    public boolean isCommitted() {
        return commitTimestamp != null;
    }

    /**
     * Checks if this version is deleted.
     *
     * @return True if this version is deleted
     */
    public boolean isDeleted() {
        return deletedAt != null;
    }

    /**
     * Checks if this version is visible at the specified timestamp.
     *
     * @param timestamp The timestamp
     * @return True if this version is visible at the timestamp
     */
    public boolean isVisibleAt(Timestamp timestamp) {
        // A version is visible if:
        // 1. It is committed
        // 2. The commit timestamp is <= the given timestamp
        // 3. If deleted, the delete timestamp is > the given timestamp

        if (commitTimestamp == null || commitTimestamp.isAfter(timestamp)) {
            // Not committed or committed after the timestamp
            return false;
        }

        if (deletedAt != null && !deletedAt.isAfter(timestamp)) {
            // Deleted at or before the timestamp
            return false;
        }

        return true;
    }

    /**
     * Creates a new deleted version based on this version.
     *
     * @param deletedAt The timestamp when the new version was deleted
     * @return A new deleted version
     */
    public Version createDeletedVersion(Timestamp deletedAt) {
        Version deletedVersion = new Version(null, this.startTimestamp, deletedAt);
        deletedVersion.setDeletedAt(deletedAt);
        return deletedVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Version version = (Version) o;
        return Arrays.equals(value, version.value) &&
                Objects.equals(startTimestamp, version.startTimestamp) &&
                Objects.equals(createdAt, version.createdAt) &&
                Objects.equals(commitTimestamp, version.commitTimestamp) &&
                Objects.equals(deletedAt, version.deletedAt);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(startTimestamp, createdAt, commitTimestamp, deletedAt);
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "Version{" +
                "valueLength=" + (value != null ? value.length : 0) +
                ", startTimestamp=" + startTimestamp +
                ", createdAt=" + createdAt +
                ", commitTimestamp=" + commitTimestamp +
                ", deletedAt=" + deletedAt +
                '}';
    }
}
