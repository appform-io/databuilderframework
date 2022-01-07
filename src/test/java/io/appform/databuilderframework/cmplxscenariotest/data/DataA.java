package io.appform.databuilderframework.cmplxscenariotest.data;

import io.appform.databuilderframework.model.Data;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class DataA extends Data {

    public int val;

    public DataA() {
        super("A");
        this.val = (int) (Math.random() * 10);

    }

    public DataA(int val) {
        super("A");
        this.val = val;

    }

}
