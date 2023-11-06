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

import javax.transaction.xa.XAException;

public class Decoder {
    
    public static String decodeXAExceptionErrorCode(XAException ex) {
        switch (ex.errorCode) {
            case -9:
                return "XAER_OUTSIDE";
            case -8:
                return "XAER_DUPID";
            case -7:
                return "XAER_RMFAIL";
            case -6:
                return "XAER_PROTO";
            case -5:
                return "XAER_INVAL";
            case -4:
                return "XAER_NOTA";
            case -3:
                return "XAER_RMERR";
            case -2:
                return "XAER_ASYNC";
            case 5:
                return "XA_HEURMIX";
            case 6:
                return "XA_HEURRB";
            case 7:
                return "XA_HEURCOM";
            case 8:
                return "XA_HEURHAZ";
            case 100:
                return "XA_RBROLLBACK";
            case 101:
                return "XA_RBCOMMFAIL";
            case 102:
                return "XA_RBDEADLOCK";
            case 103:
                return "XA_RBINTEGRITY";
            case 104:
                return "XA_RBOTHER";
            case 105:
                return "XA_RBPROTO";
            case 106:
                return "XA_RBTIMEOUT";
            case 107:
                return "XA_RBTRANSIENT";
            default:
                return "!invalid error code (" + ex.errorCode + ")!";
        }
    }
    
    public static String decodeStatus(int status) {
        switch (status) {
            case 0:
                return "ACTIVE";
            case 1:
                return "MARKED_ROLLBACK";
            case 2:
                return "PREPARED";
            case 3:
                return "COMMITTED";
            case 4:
                return "ROLLEDBACK";
            case 5:
                return "UNKNOWN";
            case 6:
                return "NO_TRANSACTION";
            case 7:
                return "PREPARING";
            case 8:
                return "COMMITTING";
            case 9:
                return "ROLLING_BACK";
            default:
                return "!incorrect status (" + status + ")!";
        }
    }
    
    public static String decodeXAResourceFlag(int flag) {
        switch (flag) {
            case 0:
                return "NOFLAGS";
            case 2097152:
                return "JOIN";
            case 8388608:
                return "ENDRSCAN";
            case 16777216:
                return "STARTRSCAN";
            case 33554432:
                return "SUSPEND";
            case 67108864:
                return "SUCCESS";
            case 134217728:
                return "RESUME";
            case 536870912:
                return "FAIL";
            case 1073741824:
                return "ONEPHASE";
            default:
                return "!invalid flag (" + flag + ")!";
        }
    }
    
    public static String decodePrepareVote(int vote) {
        switch (vote) {
            case 0:
                return "XA_OK";
            case 3:
                return "XA_RDONLY";
            default:
                return "!invalid return code (" + vote + ")!";
        }
    }
    
    public static String decodeHeaderState(byte state) {
        switch (state) {
            case -1:
                return "UNCLEAN_LOG_STATE";
            case 0:
                return "CLEAN_LOG_STATE";
            default:
                return "!invalid state (" + state + ")!";
        }
    }
    
    public static String decodeXAStatefulHolderState(int state) {
        switch (state) {
            case 0:
                return "CLOSED";
            case 1:
                return "IN_POOL";
            case 2:
                return "ACCESSIBLE";
            case 3:
                return "NOT_ACCESSIBLE";
            default:
                return "!invalid state (" + state + ")!";
        }
    }
}
