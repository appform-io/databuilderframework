package examples.bloghomepagebuilder.builders;

import examples.bloghomepagebuilder.data.*;
import io.appform.databuilderframework.annotations.DataBuilderClassInfo;
import io.appform.databuilderframework.engine.DataBuilder;
import io.appform.databuilderframework.engine.DataBuilderContext;
import io.appform.databuilderframework.engine.DataBuilderException;
import io.appform.databuilderframework.model.Data;
import lombok.val;

@DataBuilderClassInfo(produces = HomePageResponse.class, consumes = {UserDetails.class, BlogPost.class, PostList.class, FollowerList.class, RecommendedTags.class})
public class HomePageBuilder extends DataBuilder {

    public HomePageBuilder() {

    }

    @Override
    public Data process(DataBuilderContext context) throws DataBuilderException {
        val dataSetAccessor = context.getDataSet().accessor();
        return new HomePageResponse(
                        dataSetAccessor.get(UserDetails.class).getUserName(),
                        dataSetAccessor.get(BlogPost.class).getTitle(),
                        dataSetAccessor.get(BlogPost.class).getBody(),
                        dataSetAccessor.get(FollowerList.class).getFollower(),
                        dataSetAccessor.get(PostList.class).getPostTitles(),
                        dataSetAccessor.get(RecommendedTags.class).getTags()
                    );
    }
}
