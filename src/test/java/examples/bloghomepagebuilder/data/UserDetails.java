package examples.bloghomepagebuilder.data;

import io.appform.databuilderframework.model.DataAdapter;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class UserDetails extends DataAdapter<UserDetails> {
    String userName;
    int accessPermissionsFlags;
    long joinDate;

    public UserDetails(String userName, int accessPermissionsFlags, long joinDate) {
        super(UserDetails.class);
        this.userName = userName;
        this.accessPermissionsFlags = accessPermissionsFlags;
        this.joinDate = joinDate;
    }
}
