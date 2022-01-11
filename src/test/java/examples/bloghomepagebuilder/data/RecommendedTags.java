package examples.bloghomepagebuilder.data;

import io.appform.databuilderframework.model.DataAdapter;
import lombok.EqualsAndHashCode;
import lombok.Value;

import java.util.List;

@Value
@EqualsAndHashCode(callSuper = true)
public class RecommendedTags extends DataAdapter<RecommendedTags> {
    List<String> tags;

    public RecommendedTags(List<String> tags) {
        super(RecommendedTags.class);
        this.tags = tags;
    }
}
