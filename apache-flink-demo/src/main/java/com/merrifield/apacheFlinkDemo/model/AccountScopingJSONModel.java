package com.merrifield.apacheFlinkDemo.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class AccountScopingJSONModel {
    int accountNumber;
    String eventTimestamp;
    boolean inScope;
}
