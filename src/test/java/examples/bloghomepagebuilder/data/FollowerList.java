package examples.bloghomepagebuilder.data;

import io.appform.databuilderframework.model.DataAdapter;
import lombok.EqualsAndHashCode;
import lombok.Value;

import java.util.List;

@Value
@EqualsAndHashCode(callSuper = true)
public class FollowerList extends DataAdapter<FollowerList> {
    List<String> follower;

    public FollowerList(List<String> follower) {
        super(FollowerList.class);
        this.follower = follower;
    }
}
