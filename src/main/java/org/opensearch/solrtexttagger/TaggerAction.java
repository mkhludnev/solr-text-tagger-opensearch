package org.opensearch.solrtexttagger;

import org.opensearch.action.ActionType;
import org.opensearch.common.ParseField;
import org.opensearch.common.xcontent.ObjectParser;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;

public class TaggerAction extends ActionType<TaggerResponse> {
    public static TaggerAction INSTANCE = new TaggerAction();

    public static final String NAME = "indices:data/read/tagger/list";

    private TaggerAction() {
        super(NAME, TaggerResponse::new);
    }

    public static TaggerRequest fromXContent(XContentParser parser, String ... indices) throws IOException {
        TaggerRequest  req = new TaggerRequest(indices);
        PARSER.parse(parser, req, null);
        return req;
    }

    private static final ObjectParser<TaggerRequest, Void> PARSER = new ObjectParser<TaggerRequest, Void>("tagger_request");
    static {
        PARSER.declareString(TaggerRequest::field, new ParseField("field"));
        PARSER.declareString(TaggerRequest::string, new ParseField("string"));
    }
}
