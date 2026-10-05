package com.distributedkv.client;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a session with the distributed key-value store.
 * Sessions are used to maintain state between the client and the server,
 * track client-specific context, and handle session-based operations.
 */
public class Session {

    // Unique identifier for the session
    private final String sessionId;

    // Timestamp when the session was created
    private final long creationTime;

    // Timestamp when the session was last accessed
    private final AtomicLong lastAccessTime;

    // Session attributes
    private final java.util.Map<String, Object> attributes;

    /**
     * Creates a new session with the given session ID.
     *
     * @param sessionId The session ID
     */
    public Session(String sessionId) {
        this.sessionId = sessionId;
        this.creationTime = System.currentTimeMillis();
        this.lastAccessTime = new AtomicLong(this.creationTime);
        this.attributes = new java.util.concurrent.ConcurrentHashMap<>();
    }

    /**
     * Gets the session ID.
     *
     * @return The session ID
     */
    public String getSessionId() {
        return sessionId;
    }

    /**
     * Gets the creation time of the session.
     *
     * @return The creation time in milliseconds since the epoch
     */
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Gets the last access time of the session.
     *
     * @return The last access time in milliseconds since the epoch
     */
    public long getLastAccessTime() {
        return lastAccessTime.get();
    }

    /**
     * Updates the last access time of the session to the current time.
     */
    public void updateLastAccessTime() {
        lastAccessTime.set(System.currentTimeMillis());
    }

    /**
     * Gets the age of the session.
     *
     * @return The age in milliseconds
     */
    public long getAge() {
        return System.currentTimeMillis() - creationTime;
    }

    /**
     * Gets the idle time of the session.
     *
     * @return The idle time in milliseconds
     */
    public long getIdleTime() {
        return System.currentTimeMillis() - lastAccessTime.get();
    }

    /**
     * Sets an attribute in the session.
     *
     * @param name The attribute name
     * @param value The attribute value
     */
    public void setAttribute(String name, Object value) {
        if (name == null) {
            throw new IllegalArgumentException("Attribute name cannot be null");
        }

        attributes.put(name, value);
        updateLastAccessTime();
    }

    /**
     * Gets an attribute from the session.
     *
     * @param name The attribute name
     * @return The attribute value, or null if not found
     */
    public Object getAttribute(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Attribute name cannot be null");
        }

        updateLastAccessTime();
        return attributes.get(name);
    }

    /**
     * Removes an attribute from the session.
     *
     * @param name The attribute name
     * @return The attribute value, or null if not found
     */
    public Object removeAttribute(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Attribute name cannot be null");
        }

        updateLastAccessTime();
        return attributes.remove(name);
    }

    /**
     * Gets all attribute names in the session.
     *
     * @return A set of attribute names
     */
    public java.util.Set<String> getAttributeNames() {
        updateLastAccessTime();
        return java.util.Collections.unmodifiableSet(attributes.keySet());
    }

    /**
     * Clears all attributes in the session.
     */
    public void clearAttributes() {
        attributes.clear();
        updateLastAccessTime();
    }

    /**
     * Checks if the session has the specified attribute.
     *
     * @param name The attribute name
     * @return true if the session has the attribute, false otherwise
     */
    public boolean hasAttribute(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Attribute name cannot be null");
        }

        updateLastAccessTime();
        return attributes.containsKey(name);
    }

    /**
     * Returns a string representation of the session.
     *
     * @return A string representation of the session
     */
    @Override
    public String toString() {
        return "Session{" +
               "sessionId='" + sessionId + '\'' +
               ", creationTime=" + creationTime +
               ", lastAccessTime=" + lastAccessTime.get() +
               ", attributeCount=" + attributes.size() +
               '}';
    }

    /**
     * Determines if this session is equal to another object.
     * Two sessions are equal if they have the same session ID.
     *
     * @param obj The object to compare with
     * @return true if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Session other = (Session) obj;
        return sessionId.equals(other.sessionId);
    }

    /**
     * Returns a hash code for this session.
     *
     * @return A hash code for this session
     */
    @Override
    public int hashCode() {
        return sessionId.hashCode();
    }
}
