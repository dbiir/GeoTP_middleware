package org.apache.shardingsphere.infra.statistics.monitor;

import java.util.HashMap;

class LRUCache {
    private HashMap<Integer, LRUCacheNode> cache;
    private int capacity;
    private LRUCacheNode head, tail;
    private int size;

    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.size = 0;
        this.cache = new HashMap<>();
        head = new LRUCacheNode(-1, null);  // virtual head node
        tail = new LRUCacheNode(-1, null);
        head.setNext(tail);
        tail.setPrev(head);
    }

    public LockMetaData get(int key) {
        LRUCacheNode node = cache.get(key);
        if (node == null) {
            return null;
        }
        moveToHead(node);
        return node.getLockMetaData();
    }

    public void put(int key, LockMetaData value) {
        LRUCacheNode node = cache.get(key);
        if (node == null) {
            LRUCacheNode newLRUCacheNode = new LRUCacheNode(key, value);
            cache.put(key, newLRUCacheNode);
            addToHead(newLRUCacheNode);
            size++;

            if (size > capacity) {
                LRUCacheNode tail = removeTail();
                if (tail == null) {
                    increaseCapacity();  // increase capacity
                } else {
                    cache.remove(tail.getKey());  // remove from hash map
                    size--;
                }
            }
        } else {
            node.setLockMetaData(value);
            moveToHead(node);
        }
    }

    private void moveToHead(LRUCacheNode node) {
        removeLRUCacheNode(node);
        addToHead(node);
    }

    private void addToHead(LRUCacheNode node) {
        node.setPrev(head);
        node.setNext(head.getNext());
        head.getNext().setPrev(node);
        head.setNext(node);
    }

    private void removeLRUCacheNode(LRUCacheNode node) {
        node.getPrev().setNext(node.getNext());
        node.getNext().setPrev(node.getPrev());
    }

    // skip the key with processing > 0
    private LRUCacheNode removeTail() {
        LRUCacheNode cur = tail.getPrev();
        while (cur != head) {
            if (cur.canBeRemoved()) {
                removeLRUCacheNode(cur);
                return cur;
            }
            cur = cur.getPrev();
        }
        return null;
    }

    private void increaseCapacity() {
        System.out.println("All nodes are pinned, increasing capacity...");
        this.capacity += 128;  // add capacity by 128
    }
}
