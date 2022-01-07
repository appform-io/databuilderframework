package examples.bloghomepagebuilder.data;

import io.appform.databuilderframework.model.DataAdapter;
import lombok.EqualsAndHashCode;
import lombok.Value;

import java.util.List;


@Value
@EqualsAndHashCode(callSuper = true)
public class HomePageResponse extends DataAdapter<HomePageResponse> {
    String userName;
    String title;
    byte[] latestBody;
    List<String> followers;
    List<String> posts;
    List<String> tags;

    public HomePageResponse(String userName, String title, byte[] latestBody, List<String> followers, List<String> posts, List<String> tags) {
        super(HomePageResponse.class);
        this.userName = userName;
        this.title = title;
        this.latestBody = latestBody;
        this.followers = followers;
        this.posts = posts;
        this.tags = tags;
    }
}
