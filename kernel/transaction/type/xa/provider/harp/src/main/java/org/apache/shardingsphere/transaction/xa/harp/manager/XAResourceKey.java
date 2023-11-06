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

import lombok.Getter;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

public class XAResourceKey {
    
    @Getter
    private XAResource xares;
    
    public XAResourceKey(XAResource xares) {
        this.xares = xares;
    }
    
    public boolean equals(Object o) {
        boolean ret = false;
        if (o instanceof XAResourceKey) {
            XAResourceKey other = (XAResourceKey) o;
            
            try {
                ret = other.xares == this.xares || other.xares.isSameRM(this.xares);
            } catch (XAException var5) {
            }
        }
        
        return ret;
    }
    
    public int hashCode() {
        return this.xares.getClass().getName().toString().hashCode();
    }
    
    public String toString() {
        return this.xares.toString();
    }
}
