package examples.bloghomepagebuilder.data;

import io.appform.databuilderframework.model.DataAdapter;
import lombok.EqualsAndHashCode;
import lombok.Value;

import java.util.List;

@Value
@EqualsAndHashCode(callSuper = true)
public class PostList extends DataAdapter<PostList> {
    List<String> postTitles;

    public PostList(List<String> postTitles) {
        super(PostList.class);
        this.postTitles = postTitles;
    }
}
