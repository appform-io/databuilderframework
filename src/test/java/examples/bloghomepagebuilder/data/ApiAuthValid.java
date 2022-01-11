package examples.bloghomepagebuilder.data;

import io.appform.databuilderframework.model.DataAdapter;
import lombok.EqualsAndHashCode;
import lombok.Value;

@EqualsAndHashCode(callSuper = true)
@Value
public class ApiAuthValid extends DataAdapter<ApiAuthValid> {
    boolean valid;
    String decryptedUserId;

    public ApiAuthValid(boolean valid, String decryptedUserId) {
        super(ApiAuthValid.class);
        this.valid = valid;
        this.decryptedUserId = decryptedUserId;
    }
}
