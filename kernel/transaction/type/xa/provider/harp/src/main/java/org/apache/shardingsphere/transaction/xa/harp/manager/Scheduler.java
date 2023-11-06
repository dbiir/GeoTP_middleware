/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.transaction.xa.harp.manager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class Scheduler<T> implements Iterable<T> {
    
    public static final Integer DEFAULT_POSITION = 0;
    public static final Integer ALWAYS_FIRST_POSITION = Integer.MIN_VALUE;
    public static final Integer ALWAYS_LAST_POSITION = Integer.MAX_VALUE;
    private List<Integer> keys = new ArrayList();
    private Map<Integer, List<T>> objects = new TreeMap();
    private int size = 0;
    
    public Scheduler() {
    }
    
    public synchronized void add(T obj, Integer position) {
        List<T> list = (List) this.objects.get(position);
        if (list == null) {
            if (!this.keys.contains(position)) {
                this.keys.add(position);
                Collections.sort(this.keys);
            }
            
            list = new ArrayList();
            this.objects.put(position, list);
        }
        
        ((List) list).add(obj);
        ++this.size;
    }
    
    public synchronized void remove(T obj) {
        Iterator<T> it = this.iterator();
        
        Object o;
        do {
            if (!it.hasNext()) {
                throw new NoSuchElementException("no such element: " + obj);
            }
            
            o = it.next();
        } while (o != obj);
        
        it.remove();
    }
    
    public synchronized SortedSet<Integer> getNaturalOrderPositions() {
        return new TreeSet(this.objects.keySet());
    }
    
    public synchronized SortedSet<Integer> getReverseOrderPositions() {
        TreeSet<Integer> result = new TreeSet(Collections.reverseOrder());
        result.addAll(this.getNaturalOrderPositions());
        return result;
    }
    
    public synchronized List<T> getByNaturalOrderForPosition(Integer position) {
        return (List) this.objects.get(position);
    }
    
    public synchronized List<T> getByReverseOrderForPosition(Integer position) {
        List<T> result = new ArrayList(this.getByNaturalOrderForPosition(position));
        Collections.reverse(result);
        return result;
    }
    
    public synchronized int size() {
        return this.size;
    }
    
    public Iterator<T> iterator() {
        return new SchedulerNaturalOrderIterator();
    }
    
    public Iterator<T> reverseIterator() {
        return new SchedulerReverseOrderIterator();
    }
    
    public String toString() {
        return "a Scheduler with " + this.size() + " object(s) in " + this.getNaturalOrderPositions().size() + " position(s)";
    }
    
    private final class SchedulerReverseOrderIterator implements Iterator<T> {
        
        private int nextKeyIndex;
        private List<T> objectsOfCurrentKey;
        private int objectsOfCurrentKeyIndex;
        
        private SchedulerReverseOrderIterator() {
            this.nextKeyIndex = Scheduler.this.keys.size() - 1;
        }
        
        public void remove() {
            synchronized (Scheduler.this) {
                if (this.objectsOfCurrentKey == null) {
                    throw new NoSuchElementException("iterator not yet placed on an element");
                } else {
                    --this.objectsOfCurrentKeyIndex;
                    this.objectsOfCurrentKey.remove(this.objectsOfCurrentKeyIndex);
                    if (this.objectsOfCurrentKey.size() == 0) {
                        Integer key = (Integer) Scheduler.this.keys.get(this.nextKeyIndex + 1);
                        Scheduler.this.keys.remove(this.nextKeyIndex + 1);
                        Scheduler.this.objects.remove(key);
                        this.objectsOfCurrentKey = null;
                    }
                    
                    Scheduler.this.size--;
                }
            }
        }
        
        public boolean hasNext() {
            synchronized (Scheduler.this) {
                if (this.objectsOfCurrentKey != null && this.objectsOfCurrentKeyIndex < this.objectsOfCurrentKey.size()) {
                    return true;
                } else if (this.nextKeyIndex >= 0) {
                    Integer currentKey = (Integer) Scheduler.this.keys.get(this.nextKeyIndex--);
                    this.objectsOfCurrentKey = (List) Scheduler.this.objects.get(currentKey);
                    this.objectsOfCurrentKeyIndex = 0;
                    return true;
                } else {
                    return false;
                }
            }
        }
        
        public T next() {
            synchronized (Scheduler.this) {
                if (!this.hasNext()) {
                    throw new NoSuchElementException("iterator bounds reached");
                } else {
                    return this.objectsOfCurrentKey.get(this.objectsOfCurrentKeyIndex++);
                }
            }
        }
    }
    
    private final class SchedulerNaturalOrderIterator implements Iterator<T> {
        
        private int nextKeyIndex;
        private List<T> objectsOfCurrentKey;
        private int objectsOfCurrentKeyIndex;
        
        private SchedulerNaturalOrderIterator() {
            this.nextKeyIndex = 0;
        }
        
        public void remove() {
            synchronized (Scheduler.this) {
                if (this.objectsOfCurrentKey == null) {
                    throw new NoSuchElementException("iterator not yet placed on an element");
                } else {
                    --this.objectsOfCurrentKeyIndex;
                    this.objectsOfCurrentKey.remove(this.objectsOfCurrentKeyIndex);
                    if (this.objectsOfCurrentKey.size() == 0) {
                        --this.nextKeyIndex;
                        Integer key = (Integer) Scheduler.this.keys.get(this.nextKeyIndex);
                        Scheduler.this.keys.remove(this.nextKeyIndex);
                        Scheduler.this.objects.remove(key);
                        this.objectsOfCurrentKey = null;
                    }
                    
                    Scheduler.this.size--;
                }
            }
        }
        
        public boolean hasNext() {
            synchronized (Scheduler.this) {
                if (this.objectsOfCurrentKey != null && this.objectsOfCurrentKeyIndex < this.objectsOfCurrentKey.size()) {
                    return true;
                } else if (this.nextKeyIndex < Scheduler.this.keys.size()) {
                    Integer currentKey = (Integer) Scheduler.this.keys.get(this.nextKeyIndex++);
                    this.objectsOfCurrentKey = (List) Scheduler.this.objects.get(currentKey);
                    this.objectsOfCurrentKeyIndex = 0;
                    return true;
                } else {
                    return false;
                }
            }
        }
        
        public T next() {
            synchronized (Scheduler.this) {
                if (!this.hasNext()) {
                    throw new NoSuchElementException("iterator bounds reached");
                } else {
                    return this.objectsOfCurrentKey.get(this.objectsOfCurrentKeyIndex++);
                }
            }
        }
    }
}
