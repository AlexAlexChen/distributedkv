package com.distributedkv.storage.engines.btree;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implementation of a B+ tree data structure.
 *
 * A B+ tree is a self-balancing tree data structure that maintains sorted data
 * and allows for efficient insertions, deletions, and searches. All data is
 * stored in the leaf nodes, which are linked together for efficient range queries.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public class BPlusTree implements Serializable {
    private static final long serialVersionUID = 1L;

    /** The root node of the tree */
    private BPlusTreeNode root;

    /** The order of the tree (maximum number of children per node) */
    private final int order;

    /** The key comparator */
    private final BPlusTreeNode.KeyComparator comparator;

    /** Lock for thread safety */
    private final transient ReadWriteLock lock = new ReentrantReadWriteLock();

    /** The number of key-value pairs in the tree */
    private int size;

    /**
     * Default key comparator that compares keys lexicographically.
     */
    private static final BPlusTreeNode.KeyComparator DEFAULT_COMPARATOR = (key1, key2) -> {
        int len1 = key1.length;
        int len2 = key2.length;
        int lim = Math.min(len1, len2);

        for (int i = 0; i < lim; i++) {
            int a = key1[i] & 0xff;
            int b = key2[i] & 0xff;
            if (a != b) {
                return a - b;
            }
        }

        return len1 - len2;
    };

    /**
     * Creates a new B+ tree with the default order and comparator.
     */
    public BPlusTree() {
        this(4, DEFAULT_COMPARATOR);
    }

    /**
     * Creates a new B+ tree with the specified order and default comparator.
     *
     * @param order The order of the tree
     */
    public BPlusTree(int order) {
        this(order, DEFAULT_COMPARATOR);
    }

    /**
     * Creates a new B+ tree with the specified order and comparator.
     *
     * @param order The order of the tree
     * @param comparator The key comparator
     */
    public BPlusTree(int order, BPlusTreeNode.KeyComparator comparator) {
        if (order < 3) {
            throw new IllegalArgumentException("Order must be at least 3");
        }

        this.order = order;
        this.comparator = comparator;
        this.root = new BPlusTreeNode(); // Create an empty leaf node as root
        this.size = 0;
    }

    /**
     * Gets the value associated with the specified key.
     *
     * @param key The key
     * @return The value, or null if the key was not found
     */
    public byte[] get(byte[] key) {
        lock.readLock().lock();
        try {
            return search(root, key);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Searches for a key in the tree starting from the specified node.
     *
     * @param node The node to start the search from
     * @param key The key to search for
     * @return The value associated with the key, or null if not found
     */
    private byte[] search(BPlusTreeNode node, byte[] key) {
        if (node.isLeaf()) {
            // Search in leaf node
            int i = 0;
            while (i < node.getKeyCount() && comparator.compare(key, node.getKey(i)) > 0) {
                i++;
            }

            if (i < node.getKeyCount() && comparator.compare(key, node.getKey(i)) == 0) {
                return node.getValue(i);
            }

            return null;
        } else {
            // Search in internal node
            int i = 0;
            while (i < node.getKeyCount() && comparator.compare(key, node.getKey(i)) >= 0) {
                i++;
            }

            return search(node.getChild(i), key);
        }
    }

    /**
     * Puts a key-value pair into the tree.
     *
     * @param key The key
     * @param value The value
     * @return The previous value associated with the key, or null if the key was not found
     */
    public byte[] put(byte[] key, byte[] value) {
        lock.writeLock().lock();
        try {
            byte[] oldValue = search(root, key);

            // Insert the key-value pair
            BPlusTreeNode newChild = insert(root, key, value);

            // If the root was split, create a new root
            if (newChild != null) {
                BPlusTreeNode newRoot = new BPlusTreeNode(false);
                newRoot.getKeys().add(newChild.getKey(0));
                newRoot.getChildren().add(root);
                newRoot.getChildren().add(newChild);
                root = newRoot;
            }

            // Update size if this is a new key
            if (oldValue == null) {
                size++;
            }

            return oldValue;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Inserts a key-value pair into the tree starting from the specified node.
     *
     * @param node The node to start the insertion from
     * @param key The key
     * @param value The value
     * @return The new right node if the node was split, or null otherwise
     */
    private BPlusTreeNode insert(BPlusTreeNode node, byte[] key, byte[] value) {
        if (node.isLeaf()) {
            // Insert into leaf node
            node.insertValue(key, value, comparator);

            // Split if necessary
            if (node.getKeyCount() >= order) {
                return node.split(order);
            }

            return null;
        } else {
            // Find the child to insert into
            int i = 0;
            while (i < node.getKeyCount() && comparator.compare(key, node.getKey(i)) >= 0) {
                i++;
            }

            // Insert into the child
            BPlusTreeNode newChild = insert(node.getChild(i), key, value);

            // If the child was split, insert the new key and child
            if (newChild != null) {
                node.insertChild(newChild.getKey(0), newChild, comparator);

                // Split if necessary
                if (node.getKeyCount() >= order) {
                    return node.split(order);
                }
            }

            return null;
        }
    }

    /**
     * Removes a key-value pair from the tree.
     *
     * @param key The key
     * @return The value associated with the key, or null if the key was not found
     */
    public byte[] remove(byte[] key) {
        lock.writeLock().lock();
        try {
            byte[] oldValue = search(root, key);

            if (oldValue == null) {
                return null;
            }

            // Remove the key-value pair
            boolean removed = remove(root, null, 0, key);

            // Update size if the key was removed
            if (removed) {
                size--;
            }

            // If the root has only one child, make that child the new root
            if (!root.isLeaf() && root.getKeyCount() == 0) {
                root = root.getChild(0);
            }

            return oldValue;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Removes a key-value pair from the tree starting from the specified node.
     *
     * @param node The node to start the removal from
     * @param parent The parent node
     * @param parentIndex The index of this node in the parent
     * @param key The key
     * @return True if the key was removed, false otherwise
     */
    private boolean remove(BPlusTreeNode node, BPlusTreeNode parent, int parentIndex, byte[] key) {
        if (node.isLeaf()) {
            // Remove from leaf node
            boolean removed = node.removeValue(key, comparator);

            // Handle underflow
            if (removed && parent != null && node.getKeyCount() < (order / 2)) {
                handleLeafUnderflow(node, parent, parentIndex);
            }

            return removed;
        } else {
            // Find the child to remove from
            int i = 0;
            while (i < node.getKeyCount() && comparator.compare(key, node.getKey(i)) > 0) {
                i++;
            }

            // If found the exact key in an internal node, need to replace it
            if (i < node.getKeyCount() && comparator.compare(key, node.getKey(i)) == 0) {
                // Replace with the smallest key in the right subtree
                BPlusTreeNode rightChild = node.getChild(i + 1);
                BPlusTreeNode current = rightChild;

                while (!current.isLeaf()) {
                    current = current.getChild(0);
                }

                // Found the smallest key
                byte[] replacementKey = current.getKey(0);
                node.getKeys().set(i, replacementKey);

                // Continue removal in the right subtree
                return remove(rightChild, node, i + 1, key);
            } else {
                // Remove from the appropriate child
                boolean removed = remove(node.getChild(i), node, i, key);

                // Handle underflow
                if (removed && node.getChild(i).getKeyCount() < (order / 2)) {
                    handleInternalUnderflow(node, i);
                }

                return removed;
            }
        }
    }

    /**
     * Handles underflow in a leaf node by borrowing from siblings or merging.
     *
     * @param node The underflowing leaf node
     * @param parent The parent node
     * @param parentIndex The index of this node in the parent
     */
    private void handleLeafUnderflow(BPlusTreeNode node, BPlusTreeNode parent, int parentIndex) {
        // Try to borrow from the right sibling
        if (parentIndex < parent.getKeyCount()) {
            BPlusTreeNode rightSibling = parent.getChild(parentIndex + 1);

            if (rightSibling.getKeyCount() > (order / 2)) {
                // Borrow from right sibling
                byte[] newParentKey = node.redistribute(rightSibling, true, null);
                parent.getKeys().set(parentIndex, newParentKey);
                return;
            }
        }

        // Try to borrow from the left sibling
        if (parentIndex > 0) {
            BPlusTreeNode leftSibling = parent.getChild(parentIndex - 1);

            if (leftSibling.getKeyCount() > (order / 2)) {
                // Borrow from left sibling
                byte[] newParentKey = node.redistribute(leftSibling, false, null);
                parent.getKeys().set(parentIndex - 1, newParentKey);
                return;
            }
        }

        // Merge with a sibling
        if (parentIndex < parent.getKeyCount()) {
            // Merge with right sibling
            BPlusTreeNode rightSibling = parent.getChild(parentIndex + 1);
            node.merge(rightSibling, null);
            parent.removeChild(parentIndex);
        } else {
            // Merge with left sibling
            BPlusTreeNode leftSibling = parent.getChild(parentIndex - 1);
            leftSibling.merge(node, null);
            parent.removeChild(parentIndex - 1);
        }
    }

    /**
     * Handles underflow in an internal node by borrowing from siblings or merging.
     *
     * @param parent The parent node
     * @param childIndex The index of the underflowing child
     */
    private void handleInternalUnderflow(BPlusTreeNode parent, int childIndex) {
        BPlusTreeNode child = parent.getChild(childIndex);

        // Try to borrow from the right sibling
        if (childIndex < parent.getKeyCount()) {
            BPlusTreeNode rightSibling = parent.getChild(childIndex + 1);

            if (rightSibling.getKeyCount() > (order / 2)) {
                // Borrow from right sibling
                byte[] parentKey = parent.getKey(childIndex);
                byte[] newParentKey = child.redistribute(rightSibling, true, parentKey);
                parent.getKeys().set(childIndex, newParentKey);
                return;
            }
        }

        // Try to borrow from the left sibling
        if (childIndex > 0) {
            BPlusTreeNode leftSibling = parent.getChild(childIndex - 1);

            if (leftSibling.getKeyCount() > (order / 2)) {
                // Borrow from left sibling
                byte[] parentKey = parent.getKey(childIndex - 1);
                byte[] newParentKey = child.redistribute(leftSibling, false, parentKey);
                parent.getKeys().set(childIndex - 1, newParentKey);
                return;
            }
        }

        // Merge with a sibling
        if (childIndex < parent.getKeyCount()) {
            // Merge with right sibling
            BPlusTreeNode rightSibling = parent.getChild(childIndex + 1);
            byte[] parentKey = parent.getKey(childIndex);
            child.merge(rightSibling, parentKey);
            parent.removeChild(childIndex);
        } else {
            // Merge with left sibling
            BPlusTreeNode leftSibling = parent.getChild(childIndex - 1);
            byte[] parentKey = parent.getKey(childIndex - 1);
            leftSibling.merge(child, parentKey);
            parent.removeChild(childIndex - 1);
        }
    }

    /**
     * Gets the size of the tree (number of key-value pairs).
     *
     * @return The size
     */
    public int size() {
        lock.readLock().lock();
        try {
            return size;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Checks if the tree is empty.
     *
     * @return True if the tree is empty
     */
    public boolean isEmpty() {
        lock.readLock().lock();
        try {
            return size == 0;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Clears all entries from the tree.
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            root = new BPlusTreeNode();
            size = 0;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Returns an iterator over all entries in the tree.
     *
     * @return An iterator over all entries
     */
    public Iterator<Map.Entry<byte[], byte[]>> entryIterator() {
        lock.readLock().lock();
        try {
            // Find the leftmost leaf
            BPlusTreeNode node = root;
            while (!node.isLeaf()) {
                node = node.getChild(0);
            }

            // Create and return an iterator
            BPlusTreeNode firstLeaf = node;
            lock.readLock().unlock();

            return new Iterator<Map.Entry<byte[], byte[]>>() {
                private BPlusTreeNode currentNode = firstLeaf;
                private int currentIndex = 0;

                @Override
                public boolean hasNext() {
                    lock.readLock().lock();
                    try {
                        return currentNode != null &&
                            (currentIndex < currentNode.getKeyCount() || currentNode.getNextLeaf() != null);
                    } finally {
                        lock.readLock().unlock();
                    }
                }

                @Override
                public Map.Entry<byte[], byte[]> next() {
                    lock.readLock().lock();
                    try {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }

                        // If we've reached the end of the current node, move to the next leaf
                        if (currentIndex >= currentNode.getKeyCount()) {
                            currentNode = currentNode.getNextLeaf();
                            currentIndex = 0;
                        }

                        // Get the key and value
                        byte[] key = currentNode.getKey(currentIndex);
                        byte[] value = currentNode.getValue(currentIndex);
                        currentIndex++;

                        return new AbstractMap.SimpleImmutableEntry<>(key, value);
                    } finally {
                        lock.readLock().unlock();
                    }
                }
            };
        } catch (Exception e) {
            lock.readLock().unlock();
            throw e;
        }
    }

    /**
     * Returns an iterator over entries in a specific range.
     *
     * @param startKey The start key (inclusive)
     * @param endKey The end key (exclusive)
     * @return An iterator over entries in the specified range
     */
    public Iterator<Map.Entry<byte[], byte[]>> rangeIterator(byte[] startKey, byte[] endKey) {
        lock.readLock().lock();
        try {
            // Find the leaf containing the start key
            BPlusTreeNode node = root;
            while (!node.isLeaf()) {
                int i = 0;
                while (i < node.getKeyCount() && comparator.compare(startKey, node.getKey(i)) >= 0) {
                    i++;
                }
                node = node.getChild(i);
            }

            // Find the starting position
            int index = 0;
            while (index < node.getKeyCount() && comparator.compare(node.getKey(index), startKey) < 0) {
                index++;
            }

            // Create and return an iterator
            BPlusTreeNode startNode = node;
            int startIndex = index;
            lock.readLock().unlock();

            return new Iterator<Map.Entry<byte[], byte[]>>() {
                private BPlusTreeNode currentNode = startNode;
                private int currentIndex = startIndex;

                @Override
                public boolean hasNext() {
                    lock.readLock().lock();
                    try {
                        if (currentNode == null || currentIndex >= currentNode.getKeyCount()) {
                            return false;
                        }

                        // Check if we've reached the end key
                        if (endKey != null && comparator.compare(currentNode.getKey(currentIndex), endKey) >= 0) {
                            return false;
                        }

                        return true;
                    } finally {
                        lock.readLock().unlock();
                    }
                }

                @Override
                public Map.Entry<byte[], byte[]> next() {
                    lock.readLock().lock();
                    try {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }

                        // Get the key and value
                        byte[] key = currentNode.getKey(currentIndex);
                        byte[] value = currentNode.getValue(currentIndex);
                        currentIndex++;

                        // If we've reached the end of the current node, move to the next leaf
                        if (currentIndex >= currentNode.getKeyCount()) {
                            currentNode = currentNode.getNextLeaf();
                            currentIndex = 0;
                        }

                        return new AbstractMap.SimpleImmutableEntry<>(key, value);
                    } finally {
                        lock.readLock().unlock();
                    }
                }
            };
        } catch (Exception e) {
            lock.readLock().unlock();
            throw e;
        }
    }

    /**
     * Gets the height of the tree.
     *
     * @return The height of the tree
     */
    public int getHeight() {
        lock.readLock().lock();
        try {
            int height = 1;
            BPlusTreeNode node = root;

            while (!node.isLeaf()) {
                height++;
                node = node.getChild(0);
            }

            return height;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets the order of the tree.
     *
     * @return The order
     */
    public int getOrder() {
        return order;
    }

    /**
     * Validates the structure of the tree for debugging purposes.
     *
     * @return True if the tree is valid
     */
    public boolean validate() {
        lock.readLock().lock();
        try {
            return validateNode(root, null, null);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Validates a node in the tree.
     *
     * @param node The node to validate
     * @param minKey The minimum key allowed in this node
     * @param maxKey The maximum key allowed in this node
     * @return True if the node is valid
     */
    private boolean validateNode(BPlusTreeNode node, byte[] minKey, byte[] maxKey) {
        // Check key count
        if (node != root && node.getKeyCount() < (order / 2) - 1) {
            return false;
        }

        if (node.getKeyCount() >= order) {
            return false;
        }

        // Check key order
        for (int i = 1; i < node.getKeyCount(); i++) {
            if (comparator.compare(node.getKey(i - 1), node.getKey(i)) >= 0) {
                return false;
            }
        }

        // Check key range
        if (minKey != null && node.getKeyCount() > 0 &&
            comparator.compare(minKey, node.getKey(0)) > 0) {
            return false;
        }

        if (maxKey != null && node.getKeyCount() > 0 &&
            comparator.compare(node.getKey(node.getKeyCount() - 1), maxKey) >= 0) {
            return false;
        }

        // Validate children
        if (!node.isLeaf()) {
            if (node.getChildren().size() != node.getKeyCount() + 1) {
                return false;
            }

            for (int i = 0; i <= node.getKeyCount(); i++) {
                byte[] childMinKey = (i == 0) ? minKey : node.getKey(i - 1);
                byte[] childMaxKey = (i == node.getKeyCount()) ? maxKey : node.getKey(i);

                if (!validateNode(node.getChild(i), childMinKey, childMaxKey)) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("BPlusTree{order=").append(order);
            sb.append(", size=").append(size);
            sb.append(", height=").append(getHeight());
            sb.append('}');
            return sb.toString();
        } finally {
            lock.readLock().unlock();
        }
    }
}
