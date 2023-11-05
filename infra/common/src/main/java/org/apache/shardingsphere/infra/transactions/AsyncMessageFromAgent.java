package org.apache.shardingsphere.infra.transactions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;

@Getter
@RequiredArgsConstructor
public class AsyncMessageFromAgent implements Serializable {
    private final String Xid;

    private final XATransactionState state;

    private final long currentTimeStamp;

    private final String SQLExceptionString;

    @JsonCreator
    public AsyncMessageFromAgent(@JsonProperty("state") String state, @JsonProperty("currentTimeStamp") long currentTimeStamp,
                                 @JsonProperty("xid") String xid, @JsonProperty("sqlexceptionString") String sqlexceptionString) {
        this.state = XATransactionState.valueOf(state);
        this.currentTimeStamp = currentTimeStamp;
        this.Xid = xid;
        this.SQLExceptionString = sqlexceptionString;
    }

    @Override
    public String toString() {
        return "message{" +
                "Xid=" + Xid +
                ", state=" + state +
                ", timestamp=" + currentTimeStamp +
                ", exceptionHint=" + SQLExceptionString + "}";
    }
}