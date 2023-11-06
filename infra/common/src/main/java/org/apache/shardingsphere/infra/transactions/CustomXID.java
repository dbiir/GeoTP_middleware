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

package org.apache.shardingsphere.infra.transactions;

import java.util.Arrays;
import java.util.List;

import com.mysql.jdbc.jdbc2.optional.MysqlXid;
import lombok.ToString;

import javax.transaction.xa.Xid;

@ToString
public class CustomXID {
    
    byte[] myBqual;
    int myFormatId;
    byte[] myGtrid;
    String originStr;
    
    public CustomXID(byte[] gtrid, byte[] bqual, int formatId) {
        this.myGtrid = gtrid;
        this.myBqual = bqual;
        this.myFormatId = formatId;
        this.originStr = Arrays.toString(gtrid) + "," + Arrays.toString(bqual) + "," + formatId;
    }
    
    public CustomXID(byte[] gtrid, byte[] bqual) {
        this(gtrid, bqual, 1);
    }
    
    public CustomXID(String gtrid, String bqual) {
        this(gtrid.getBytes(), bqual.getBytes());
    }
    
    public CustomXID(String str) {
        List<String> list = Arrays.asList(str.split(","));
        
        if (list.size() == 1) {
            this.myGtrid = list.get(0).getBytes();
            this.myBqual = "".getBytes();
            this.myFormatId = 1;
        } else if (list.size() == 2) {
            this.myGtrid = list.get(0).getBytes();
            this.myBqual = list.get(1).getBytes();
            this.myFormatId = 1;
        } else {
            assert (list.size() <= 3);
            this.myGtrid = list.get(0).getBytes();
            this.myBqual = list.get(1).getBytes();
            if (list.get(2).startsWith("0x")) {
                this.myFormatId = Integer.parseInt(list.get(2).substring(2), 16);
            } else {
                this.myFormatId = Integer.parseInt(list.get(2));
            }
        }
        
        originStr = str;
    }
    
    public CustomXID(Xid xid) {
        if (xid instanceof MysqlXid) {
            this.myGtrid = xid.getGlobalTransactionId();
            this.myBqual = xid.getBranchQualifier();
            this.myFormatId = xid.getFormatId();
            this.originStr = Arrays.toString(myGtrid) + "," + Arrays.toString(myBqual) + "," + myFormatId;
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        boolean ret = false;
        if (obj instanceof CustomXID) {
            CustomXID other = (CustomXID) obj;
            ret = originStr.equals(other.originStr);
        }
        
        return ret;
    }
    
    public int hashCode() {
        return (this.originStr.hashCode() / 100) * 100 + Integer.parseInt(String.valueOf(myFormatId)) % 100;
    }
    
    public String toString() {
        return this.originStr;
    }
}
