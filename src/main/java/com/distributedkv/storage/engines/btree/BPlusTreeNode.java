package com.distributedkv.storage.engines.btree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents a node in a B+ tree.
 *
 * A B+ tree node can be either an internal node (containing keys and child pointers)
 * or a leaf node (containing keys and values). Leaf nodes are linked together to
 * allow for efficient range scans.
 */
public class BPlusTreeNode implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Flag indicating whether this is a leaf node */
    private boolean leaf;

    /** The keys stored in this node */
    private List<byte[]> keys;

    /** The values stored in this node (only for leaf nodes) */
    private List<byte[]> values;

    /** The child nodes (only for internal nodes) */
    private List<BPlusTreeNode> children;

    /** The next leaf node (only for leaf nodes) */
    private BPlusTreeNode nextLeaf;

    /** The previous leaf node (only for leaf nodes) */
    private BPlusTreeNode prevLeaf;

    /**
     * Creates a new leaf node.
     */
    public BPlusTreeNode() {
        this.leaf = true;
        this.keys = new ArrayList<>();
        this.values = new ArrayList<>();
        this.children = null;
        this.nextLeaf = null;
        this.prevLeaf = null;
    }

    /**
     * Creates a new internal node.
     *
     * @param leaf Whether this is a leaf node
     */
    public BPlusTreeNode(boolean leaf) {
        this.leaf = leaf;
        this.keys = new ArrayList<>();

        if (leaf) {
            this.values = new ArrayList<>();
            this.children = null;
        } else {
            this.values = null;
            this.children = new ArrayList<>();
        }

        this.nextLeaf = null;
        this.prevLeaf = null;
    }

    /**
     * Checks if this is a leaf node.
     *
     * @return True if this is a leaf node
     */
    public boolean isLeaf() {
        return leaf;
    }

    /**
     * Gets the number of keys in this node.
     *
     * @return The number of keys
     */
    public int getKeyCount() {
        return keys.size();
    }

    /**
     * Gets the key at the specified index.
     *
     * @param index The index
     * @return The key
     */
    public byte[] getKey(int index) {
        return keys.get(index);
    }

    /**
     * Gets all keys in this node.
     *
     * @return The keys
     */
    public List<byte[]> getKeys() {
        return keys;
    }

    /**
     * Gets the value at the specified index.
     *
     * @param index The index
     * @return The value
     * @throws IllegalStateException If this is not a leaf node
     */
    public byte[] getValue(int index) {
        if (!leaf) {
            throw new IllegalStateException("Cannot get value from internal node");
        }
        return values.get(index);
    }

    /**
     * Gets all values in this node.
     *
     * @return The values
     * @throws IllegalStateException If this is not a leaf node
     */
    public List<byte[]> getValues() {
        if (!leaf) {
            throw new IllegalStateException("Cannot get values from internal node");
        }
        return values;
    }

    /**
     * Gets the child at the specified index.
     *
     * @param index The index
     * @return The child node
     * @throws IllegalStateException If this is a leaf node
     */
    public BPlusTreeNode getChild(int index) {
        if (leaf) {
            throw new IllegalStateException("Cannot get child from leaf node");
        }
        return children.get(index);
    }

    /**
     * Gets all children of this node.
     *
     * @return The children
     * @throws IllegalStateException If this is a leaf node
     */
    public List<BPlusTreeNode> getChildren() {
        if (leaf) {
            throw new IllegalStateException("Cannot get children from leaf node");
        }
        return children;
    }

    /**
     * Gets the next leaf node.
     *
     * @return The next leaf node
     * @throws IllegalStateException If this is not a leaf node
     */
    public BPlusTreeNode getNextLeaf() {
        if (!leaf) {
            throw new IllegalStateException("Cannot get next leaf from internal node");
        }
        return nextLeaf;
    }

    /**
     * Sets the next leaf node.
     *
     * @param nextLeaf The next leaf node
     * @throws IllegalStateException If this is not a leaf node
     */
    public void setNextLeaf(BPlusTreeNode nextLeaf) {
        if (!leaf) {
            throw new IllegalStateException("Cannot set next leaf for internal node");
        }
        this.nextLeaf = nextLeaf;
    }

    /**
     * Gets the previous leaf node.
     *
     * @return The previous leaf node
     * @throws IllegalStateException If this is not a leaf node
     */
    public BPlusTreeNode getPrevLeaf() {
        if (!leaf) {
            throw new IllegalStateException("Cannot get previous leaf from internal node");
        }
        return prevLeaf;
    }

    /**
     * Sets the previous leaf node.
     *
     * @param prevLeaf The previous leaf node
     * @throws IllegalStateException If this is not a leaf node
     */
    public void setPrevLeaf(BPlusTreeNode prevLeaf) {
        if (!leaf) {
            throw new IllegalStateException("Cannot set previous leaf for internal node");
        }
        this.prevLeaf = prevLeaf;
    }

    /**
     * Finds the index where a key should be inserted.
     *
     * @param key The key
     * @param comparator The key comparator
     * @return The index
     */
    public int findInsertionPoint(byte[] key, KeyComparator comparator) {
        int low = 0;
        int high = keys.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = comparator.compare(key, keys.get(mid));

            if (cmp < 0) {
                high = mid - 1;
            } else if (cmp > 0) {
                low = mid + 1;
            } else {
                return mid; // Key already exists
            }
        }

        return low;
    }

    /**
     * Inserts a key-value pair into this leaf node.
     *
     * @param key The key
     * @param value The value
     * @param comparator The key comparator
     * @return True if the key was inserted, false if it already existed
     * @throws IllegalStateException If this is not a leaf node
     */
    public boolean insertValue(byte[] key, byte[] value, KeyComparator comparator) {
        if (!leaf) {
            throw new IllegalStateException("Cannot insert value into internal node");
        }

        int insertionPoint = findInsertionPoint(key, comparator);

        // Check if the key already exists
        if (insertionPoint < keys.size() && comparator.compare(key, keys.get(insertionPoint)) == 0) {
            // Update the value
            values.set(insertionPoint, value);
            return false;
        }

        // Insert the key and value
        keys.add(insertionPoint, key);
        values.add(insertionPoint, value);
        return true;
    }

    /**
     * Inserts a key and child into this internal node.
     *
     * @param key The key
     * @param child The child node
     * @param comparator The key comparator
     * @throws IllegalStateException If this is a leaf node
     */
    public void insertChild(byte[] key, BPlusTreeNode child, KeyComparator comparator) {
        if (leaf) {
            throw new IllegalStateException("Cannot insert child into leaf node");
        }

        int insertionPoint = findInsertionPoint(key, comparator);

        // Insert the key and child
        keys.add(insertionPoint, key);
        children.add(insertionPoint + 1, child);
    }

    /**
     * Removes a key-value pair from this leaf node.
     *
     * @param key The key
     * @param comparator The key comparator
     * @return True if the key was removed, false if it was not found
     * @throws IllegalStateException If this is not a leaf node
     */
    public boolean removeValue(byte[] key, KeyComparator comparator) {
        if (!leaf) {
            throw new IllegalStateException("Cannot remove value from internal node");
        }

        int index = findInsertionPoint(key, comparator);

        // Check if the key exists
        if (index < keys.size() && comparator.compare(key, keys.get(index)) == 0) {
            // Remove the key and value
            keys.remove(index);
            values.remove(index);
            return true;
        }

        return false;
    }

    /**
     * Removes a key and child from this internal node.
     *
     * @param index The index of the key to remove
     * @throws IllegalStateException If this is a leaf node
     */
    public void removeChild(int index) {
        if (leaf) {
            throw new IllegalStateException("Cannot remove child from leaf node");
        }

        // Remove the key and child
        keys.remove(index);
        children.remove(index + 1);
    }

    /**
     * Splits this node into two nodes.
     *
     * @param order The B+ tree order
     * @return The new right node
     */
    public BPlusTreeNode split(int order) {
        int middleIndex = keys.size() / 2;

        // Create a new node
        BPlusTreeNode rightNode = new BPlusTreeNode(leaf);

        // Move half of the keys to the new node
        for (int i = middleIndex; i < keys.size(); i++) {
            rightNode.keys.add(keys.get(i));
        }

        // Remove moved keys from this node
        while (keys.size() > middleIndex) {
            keys.remove(middleIndex);
        }

        if (leaf) {
            // Move half of the values to the new node
            for (int i = middleIndex; i < values.size(); i++) {
                rightNode.values.add(values.get(i));
            }

            // Remove moved values from this node
            while (values.size() > middleIndex) {
                values.remove(middleIndex);
            }

            // Update leaf node links
            rightNode.nextLeaf = this.nextLeaf;
            rightNode.prevLeaf = this;

            if (this.nextLeaf != null) {
                this.nextLeaf.prevLeaf = rightNode;
            }

            this.nextLeaf = rightNode;
        } else {
            // Move half of the children to the new node
            for (int i = middleIndex; i < children.size(); i++) {
                rightNode.children.add(children.get(i));
            }

            // Remove moved children from this node
            while (children.size() > middleIndex + 1) {
                children.remove(middleIndex + 1);
            }
        }

        return rightNode;
    }

    /**
     * Merges this node with the specified right node.
     *
     * @param rightNode The right node to merge with
     * @param parentKey The key from the parent that separates these nodes
     * @throws IllegalStateException If the nodes cannot be merged
     */
    public void merge(BPlusTreeNode rightNode, byte[] parentKey) {
        if (this.leaf != rightNode.leaf) {
            throw new IllegalStateException("Cannot merge leaf and internal nodes");
        }

        if (leaf) {
            // Merge leaf nodes
            keys.addAll(rightNode.keys);
            values.addAll(rightNode.values);

            // Update leaf node links
            this.nextLeaf = rightNode.nextLeaf;

            if (rightNode.nextLeaf != null) {
                rightNode.nextLeaf.prevLeaf = this;
            }
        } else {
            // Merge internal nodes
            keys.add(parentKey);
            keys.addAll(rightNode.keys);
            children.addAll(rightNode.children);
        }
    }

    /**
     * Redistributes keys and values with a sibling node.
     *
     * @param sibling The sibling node
     * @param isRightSibling Whether the sibling is on the right
     * @param parentKey The key from the parent that separates these nodes
     * @return The new separator key for the parent
     */
    public byte[] redistribute(BPlusTreeNode sibling, boolean isRightSibling, byte[] parentKey) {
        if (this.leaf != sibling.leaf) {
            throw new IllegalStateException("Cannot redistribute between leaf and internal nodes");
        }

        byte[] newParentKey;

        if (leaf) {
            if (isRightSibling) {
                // Move the first key-value pair from the right sibling to this node
                keys.add(sibling.keys.get(0));
                values.add(sibling.values.get(0));

                sibling.keys.remove(0);
                sibling.values.remove(0);

                // Update the parent key
                newParentKey = Arrays.copyOf(sibling.keys.get(0), sibling.keys.get(0).length);
            } else {
                // Move the last key-value pair from the left sibling to this node
                keys.add(0, sibling.keys.get(sibling.keys.size() - 1));
                values.add(0, sibling.values.get(sibling.values.size() - 1));

                sibling.keys.remove(sibling.keys.size() - 1);
                sibling.values.remove(sibling.values.size() - 1);

                // Update the parent key
                newParentKey = Arrays.copyOf(keys.get(0), keys.get(0).length);
            }
        } else {
            if (isRightSibling) {
                // Move a key-child pair from the right sibling to this node
                keys.add(parentKey);
                children.add(sibling.children.get(0));

                // Update the parent key
                newParentKey = sibling.keys.get(0);

                sibling.keys.remove(0);
                sibling.children.remove(0);
            } else {
                // Move a key-child pair from the left sibling to this node
                keys.add(0, parentKey);
                children.add(0, sibling.children.get(sibling.children.size() - 1));

                // Update the parent key
                newParentKey = sibling.keys.get(sibling.keys.size() - 1);

                sibling.keys.remove(sibling.keys.size() - 1);
                sibling.children.remove(sibling.children.size() - 1);
            }
        }

        return newParentKey;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(leaf ? "Leaf{" : "Internal{");
        sb.append("keys=").append(keys.size());

        if (leaf) {
            sb.append(", values=").append(values.size());
            sb.append(", hasNext=").append(nextLeaf != null);
            sb.append(", hasPrev=").append(prevLeaf != null);
        } else {
            sb.append(", children=").append(children.size());
        }

        sb.append('}');
        return sb.toString();
    }

    /**
     * Interface for comparing keys.
     */
    public interface KeyComparator {
        /**
         * Compares two keys.
         *
         * @param key1 The first key
         * @param key2 The second key
         * @return A negative integer, zero, or a positive integer as the first key is less than,
         *         equal to, or greater than the second key
         */
        int compare(byte[] key1, byte[] key2);
    }
}
