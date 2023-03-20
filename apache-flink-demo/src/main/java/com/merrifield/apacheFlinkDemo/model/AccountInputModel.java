package com.merrifield.apacheFlinkDemo.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class AccountInputModel {
    int accounts;
    String owner;
    String eventTimeStamp;
    boolean endOfWindow;
}
