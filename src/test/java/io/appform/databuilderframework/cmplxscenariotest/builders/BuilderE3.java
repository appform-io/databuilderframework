package io.appform.databuilderframework.cmplxscenariotest.builders;

import io.appform.databuilderframework.annotations.DataBuilderInfo;
import io.appform.databuilderframework.cmplxscenariotest.ThreadUtils;
import io.appform.databuilderframework.cmplxscenariotest.data.DataD;
import io.appform.databuilderframework.cmplxscenariotest.data.DataE3;
import io.appform.databuilderframework.engine.DataBuilder;
import io.appform.databuilderframework.engine.DataBuilderContext;
import io.appform.databuilderframework.engine.DataBuilderException;
import io.appform.databuilderframework.engine.DataValidationException;
import io.appform.databuilderframework.model.Data;
import io.appform.databuilderframework.model.DataSet;
import lombok.val;

@DataBuilderInfo(name = "BuilderE3", accesses={"A","C"}, consumes = {"D"}, produces = "E3")
public class BuilderE3 extends DataBuilder{

	@Override
	public Data process(DataBuilderContext context)
			throws DataBuilderException, DataValidationException {
		val dataSetAccessor = DataSet.accessor(context.getDataSet());
		val dataD = dataSetAccessor.get("D", DataD.class);
		if(dataD.val <= 2){ // RUN FOR 2 VAL
			ThreadUtils.INSTANCE.putToSleep(20, "E3");
			return new DataE3();
		}
		return null;
	}

}
