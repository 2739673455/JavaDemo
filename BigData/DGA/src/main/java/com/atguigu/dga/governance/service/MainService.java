package com.atguigu.dga.governance.service;

public interface MainService {
    void startGovernanceAssess(String schemaName, String assessDate);

    void startGovernanceAssess(String assessDate);

    void startGovernanceAssess();
}
