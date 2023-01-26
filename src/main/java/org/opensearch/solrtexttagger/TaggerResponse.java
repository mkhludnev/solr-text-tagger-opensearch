package org.opensearch.solrtexttagger;

import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.BroadcastResponse;
import org.opensearch.common.ParseField;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ConstructingObjectParser;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.solrtexttagger.impl.Tag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.opensearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TaggerResponse extends BroadcastResponse {

    static final ConstructingObjectParser<TaggerResponse,Void> PARSER = new ConstructingObjectParser<>(
            "tagger_results",
            true,
            data -> {
                BroadcastResponse rsp = (BroadcastResponse) data[0];
                return new TaggerResponse((List<Tag>)data[1],// tags
                        (List<String>)data[2],// docs
                        rsp.getTotalShards(),
                        rsp.getSuccessfulShards(),
                        rsp.getFailedShards(),
                        Arrays.asList(rsp.getShardFailures()),
                        (Boolean) data[3]
                        );
            }
    );

    private static final String COMPLETE_FIELD = "complete";

    private static final String TAGS_FIELD = "tags";

    private static final String DOCS_FIELD = "docs";

    static {
        declareBroadcastFields(PARSER);
        PARSER.declareObjectArray(optionalConstructorArg(), (p,c)-> declareTags(p), new ParseField(TAGS_FIELD));
//        PARSER.declareStringArray(optionalConstructorArg(), new ParseField(TAGS_FIELD));
        PARSER.declareStringArray(optionalConstructorArg(), new ParseField(DOCS_FIELD));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField(COMPLETE_FIELD));
    }

    private static final ConstructingObjectParser<Tag, Void> TAG_PARSER = new ConstructingObjectParser<>(
            "tag",
            true,
    data -> {return new Tag((Integer) data[0], (Integer) data[1]);});
    static {
        TAG_PARSER.declareInt(optionalConstructorArg(), new ParseField("start_offset"));
        TAG_PARSER.declareInt(optionalConstructorArg(), new ParseField("end_offset"));
        TAG_PARSER.declareStringOrNull(Tag::setMatchText, new ParseField("match_text"));
        TAG_PARSER.declareStringArray(Tag::setIds,new ParseField("ids"));
    }
    private static Tag declareTags(XContentParser p) {
        return TAG_PARSER.apply(p,null);
    }

    private final List<Tag> tags;
    private final List<String> docs;
    private final boolean complete;

    public TaggerResponse(List<Tag> tags, List<String> docs, int totalShards, int successfulShards, int failedShards,
                          List<DefaultShardOperationFailedException> failures, boolean complete) {
        super(totalShards, successfulShards, failedShards, failures);
        this.tags = tags==null ? Collections.emptyList() : tags;
        this.docs = docs==null ? Collections.emptyList() : docs;
        this.complete = complete;
    }

    TaggerResponse(StreamInput in) throws IOException {
        super(in);
        final int tagz = in.readInt();
        final ArrayList<Tag> tags = new ArrayList<>();
        for (int i=0; i<tagz; i++) {
            final Tag tag = new Tag(in.readInt(), in.readInt());
            final String matchText = in.readOptionalString();
            if (!Strings.isEmpty(matchText)) {
                tag.setMatchText(matchText);
            }
            final List<String> idz = in.readStringList();
            idz.forEach(tag::addId);
            tags.add(tag);
        }
        this.tags = tags;
        this.docs = in.readStringList();
        this.complete = in.readBoolean();
    }

    public List<Tag> getTags() {
        return tags==null ? Collections.emptyList() : tags;
    }

    public List<String> getDocs() {
        return docs==null ? Collections.emptyList() : docs;
    }

    public boolean isComplete() {
        return complete;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(this.tags.size());
        for (Tag tg:this.tags){
            out.writeInt(tg.getStartOffset());
            out.writeInt(tg.getEndOffset());
            out.writeOptionalString(tg.getMatchText());
            out.writeStringCollection(tg.getIds());
        }
        out.writeStringCollection(docs);
        out.writeBoolean(complete);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(TAGS_FIELD);
        for(Tag tag : getTags()){
            builder.startObject();
            builder.field("start_offset", tag.getStartOffset());
            builder.field("end_offset", tag.getEndOffset());
            if (!Strings.isEmpty(tag.getMatchText())) {
                builder.field("match_text", tag.getMatchText());
            }
            builder.field("ids",tag.getIds());
            builder.endObject();
        }
        builder.endArray();
        builder.startArray(DOCS_FIELD);
        for(String tag : getDocs()){
            builder.value(tag);
        }
        builder.endArray();
        builder.field(COMPLETE_FIELD, complete);
    }

    public static TaggerResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return "TaggerResponse{" +
                "tags=" + tags +
                ", docs=" + docs +
                ", complete=" + complete +
                '}';
    }
}
