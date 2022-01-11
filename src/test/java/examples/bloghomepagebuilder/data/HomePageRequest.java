package examples.bloghomepagebuilder.data;

import io.appform.databuilderframework.model.DataAdapter;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class HomePageRequest extends DataAdapter<HomePageRequest>{
    String requestAuthToken;
    String userAuthToken;
    byte[] body;

    public HomePageRequest(String authToken, String userAuthToken, byte[] body) {
        super(HomePageRequest.class);
        this.requestAuthToken = authToken;
        this.userAuthToken = userAuthToken;
        this.body = body;
    }
}
