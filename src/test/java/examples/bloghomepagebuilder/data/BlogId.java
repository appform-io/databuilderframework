package examples.bloghomepagebuilder.data;

import io.appform.databuilderframework.model.DataAdapter;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class BlogId extends DataAdapter<BlogId> {
    String id;

    public BlogId(String id) {
        super(BlogId.class);
        this.id = id;
    }
}
