package org.opensearch.solrtexttagger;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class TaggerRequest extends BroadcastRequest<TaggerRequest> implements ToXContentObject {

    private String field;
    private String string = null;

    public TaggerRequest() {
        super(Strings.EMPTY_ARRAY);
    }
    public TaggerRequest(String ... indices) {
        super(indices);
        indicesOptions(SearchRequest.DEFAULT_INDICES_OPTIONS);
        timeout(new TimeValue(1000));
    }

    public TaggerRequest(TaggerRequest source){
        this.field = source.field;
        this.string = source.string;
        indices(source.indices);
        indicesOptions(source.indicesOptions());
        timeout(source.timeout());
        setParentTask(source.getParentTask());
    }

    public TaggerRequest(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.string = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(field);
        out.writeOptionalString(string);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        xContentBuilder.startObject();
        xContentBuilder.field("field", field);
        if (string != null) {
            xContentBuilder.field("string", string);
        }
        return xContentBuilder.endObject();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validate = super.validate();
        if (field == null) {
            validate = ValidateActions.addValidationError("field can't be null", validate);
        }
        return validate;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    public TaggerRequest field(String field) {
        this.field = field;
        return this;
    }

    public String field() {
        return field;
    }

    public TaggerRequest string(String string) {
        this.string = string;
        return this;
    }

    @Nullable
    public String string() {
        return string;
    }

    @Override
    public String toString() {
        return "TaggerRequest{" +
                "field='" + field + '\'' +
                ", string='" + string + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaggerRequest that = (TaggerRequest) o;
        return field.equals(that.field) && Objects.equals(string, that.string) && Objects.equals(indices, that.indices);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(field, string) + Arrays.hashCode(indices);
    }

    // @TODO
    public int size() {
        return 10;
    }
}
