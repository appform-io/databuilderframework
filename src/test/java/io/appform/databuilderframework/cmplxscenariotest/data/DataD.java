package io.appform.databuilderframework.cmplxscenariotest.data;

import io.appform.databuilderframework.model.Data;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class DataD extends Data {

    public int val;

    public DataD() {
        super("D");
        this.val = (int) (Math.random() * 5);
    }

}
