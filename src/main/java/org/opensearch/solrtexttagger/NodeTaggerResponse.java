package org.opensearch.solrtexttagger;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.solrtexttagger.impl.Tag;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NodeTaggerResponse extends TransportResponse {


    private String error;
    private boolean complete;

    private List<Tag> tags;
    private List<String> docs;
    private String nodeId;

    NodeTaggerResponse(StreamInput in) throws IOException {
        super(in);
        final int tagz = in.readInt();
        final ArrayList<Tag> consumer = new ArrayList<>();
        for (int i=0; i<tagz; i++) {
            final Tag tag = new Tag(in.readInt(), in.readInt());
            final String matchText = in.readOptionalString();
            tag.setMatchText(matchText);
            final List<String> strings = in.readStringList();
            strings.forEach(tag::addId);
            consumer.add(tag);
        }
        tags = consumer;
        docs = in.readStringList();
        error = in.readOptionalString();
        complete = in.readBoolean();
        nodeId = in.readString();
    }

    NodeTaggerResponse(String nodeId, List<Tag> tags, List<String> docs, String error, boolean complete) {
        this.nodeId = nodeId;
        this.tags = tags;
        this.docs = docs;
        this.error = error;
        this.complete = complete;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(tags.size());
        for (Tag t : tags) {
            out.writeInt(t.getStartOffset());
            out.writeInt(t.getEndOffset());
            out.writeOptionalString(t.getMatchText());
            out.writeStringCollection(t.getIds());
        }
        out.writeStringCollection(docs);
        out.writeOptionalString(error);
        out.writeBoolean(complete);
        out.writeString(nodeId);
    }

    public List<Tag> tags() {
        return this.tags;
    }

    public List<String> docs() {
        return this.docs;
    }

    public String getError() {
        return error;
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isComplete() {
        return complete;
    }
}
