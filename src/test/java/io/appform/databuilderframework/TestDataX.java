package io.appform.databuilderframework;

import io.appform.databuilderframework.model.Data;

public class TestDataX extends Data {
    private String value;

    public TestDataX(String value) {
        super("X");
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
