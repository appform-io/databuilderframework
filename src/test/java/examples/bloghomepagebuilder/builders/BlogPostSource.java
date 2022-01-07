package examples.bloghomepagebuilder.builders;


import examples.bloghomepagebuilder.data.BlogId;
import examples.bloghomepagebuilder.data.BlogPost;
import examples.bloghomepagebuilder.data.UserDetails;
import io.appform.databuilderframework.annotations.DataBuilderClassInfo;
import io.appform.databuilderframework.engine.DataBuilder;
import io.appform.databuilderframework.engine.DataBuilderContext;
import io.appform.databuilderframework.engine.DataBuilderException;
import io.appform.databuilderframework.model.Data;
import lombok.val;

@DataBuilderClassInfo(produces = BlogPost.class, consumes = {UserDetails.class, BlogId.class})
public class BlogPostSource extends DataBuilder {
    @Override
    public Data process(DataBuilderContext context) throws DataBuilderException {
        val accessor = context.getDataSet().accessor();
        val userDetails = accessor.get(UserDetails.class);
        val blogId = accessor.get(BlogId.class);
        //Get blog for this user and id
        return new BlogPost("A data depndent frame work",
                "Blah .. blah and some more blah".getBytes());
    }
}
