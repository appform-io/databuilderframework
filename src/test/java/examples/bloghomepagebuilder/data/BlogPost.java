package examples.bloghomepagebuilder.data;

import io.appform.databuilderframework.model.DataAdapter;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class BlogPost extends DataAdapter<BlogPost> {
    String title;
    byte[] body;

    public BlogPost(String title, byte[] body) {
        super(BlogPost.class);
        this.title = title;
        this.body = body;
    }
}
