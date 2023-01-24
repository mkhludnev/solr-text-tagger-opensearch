package org.opensearch.solrtexttagger;

import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

public class SolrTextTaggerHandler extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        final String path = "{index}/_solr_text_tagger";
        return  List.of(
                new Route(GET, path),
                new Route(POST, path));
    }

    @Override
    public String getName() {
        return "solr_text_tagger";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient) throws IOException {
        try(XContentParser parser = restRequest.contentOrSourceParamParser()){
            TaggerRequest taggerReq = TaggerAction.fromXContent(parser, Strings.splitStringByCommaToArray(restRequest.param("index")));
            return channel -> nodeClient.execute(TaggerAction.INSTANCE, taggerReq, new RestToXContentListener<>(channel));
        }
    }
}
