package org.apache.shardingsphere.proxy.backend.txnsails;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter
@RequiredArgsConstructor
public class PreValidationInfo {
  final String table;
  final long key;
  final LockType type;
  @Setter
  long version;
}
