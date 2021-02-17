package io.appform.databuilderframework;

import io.appform.databuilderframework.model.Data;

public class TestDataA extends Data {
    private String value;

    public TestDataA(String value) {
        super("A");
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
