package io.appform.databuilderframework;

import io.appform.databuilderframework.model.Data;

public class TestDataE extends Data {
    private String value;

    public TestDataE(String value) {
        super("E");
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
