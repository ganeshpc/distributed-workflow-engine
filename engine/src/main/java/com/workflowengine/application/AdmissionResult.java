package com.workflowengine.application;

import com.workflowengine.api.WorkflowSnapshot;

public record AdmissionResult(boolean created, WorkflowSnapshot snapshot) {
}
