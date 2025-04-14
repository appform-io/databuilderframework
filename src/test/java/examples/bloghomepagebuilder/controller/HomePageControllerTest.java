package examples.bloghomepagebuilder.controller;

import com.google.common.base.Stopwatch;
import examples.bloghomepagebuilder.builders.*;
import examples.bloghomepagebuilder.data.HomePageRequest;
import examples.bloghomepagebuilder.data.HomePageResponse;
import io.appform.databuilderframework.engine.*;
import io.appform.databuilderframework.model.DataFlow;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class HomePageControllerTest {
    //private final DataFlowExecutor executor = new MultiThreadedDataFlowExecutor(Executors.newFixedThreadPool(10));
    private final DataFlow homePageDataFlow;

    public HomePageControllerTest() throws DataBuilderFrameworkException {
        homePageDataFlow = new DataFlowBuilder()
                                .withDataBuilder(new ApiAuthChecker())
                                .withDataBuilder(new PostListBuilder())
                                .withDataBuilder(new BlogPostSource())
                                .withDataBuilder(new GetFollowers())
                                .withDataBuilder(new LatestBlogSelector())
                                .withDataBuilder(new RecommendationBuilder())
                                .withDataBuilder(new UserDataExpander())
                                .withDataBuilder(new HomePageBuilder())
                                .withTargetData(HomePageResponse.class)
                                .build();
        log.info("Dataflow: {}", homePageDataFlow);
    }

    @Test
    public void testHomePage() throws Exception {
        runHomePageTest(new SimpleDataFlowExecutor());
    }

    @Test
    public void testHomePageMT() throws Exception {
        runHomePageTest(new MultiThreadedDataFlowExecutor(Executors.newFixedThreadPool(10)));
    }

    @Test
    public void testHomePageMTOpt() throws Exception {
        runHomePageTest(new OptimizedMultiThreadedDataFlowExecutor(Executors.newFixedThreadPool(10)));
    }

    private void runHomePageTest(DataFlowExecutor executor) throws Exception {
        val request = new HomePageRequest("2321312312", "2323454", "Blah".getBytes());
        val stopwatch = Stopwatch.createStarted();
        for(long i = 0; i < 10_000_000; i++) {
            HomePageResponse response = executor.run(homePageDataFlow, request).get(HomePageResponse.class);
            Assert.assertNotNull(response);
            //System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(response));
        }
        log.info("Time taken: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
}
