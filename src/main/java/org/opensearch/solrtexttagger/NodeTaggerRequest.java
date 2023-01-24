package org.opensearch.solrtexttagger;

import org.opensearch.Version;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.util.set.Sets;
import org.opensearch.index.shard.ShardId;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

public class NodeTaggerRequest extends TransportRequest implements IndicesRequest {

    private final String field;

    private final String string;

    private final Set<ShardId> shardIds;

    private final String nodeId;

    private final long timeout;

    private final int size;
    private final OriginalIndices originalIndices;

    private long nodeStartedTimeMillis;

    private final long taskStartedTimeMillis;

    public NodeTaggerRequest(
            OriginalIndices originalIndices,
            final String nodeId,
            final Set<ShardId> shardIds,
            TaggerRequest request,
            long taskStartTimeMillis
    ) {
        this.originalIndices = originalIndices;
        this.field = request.field();
        this.string = request.string();


        this.size = request.size();
        this.timeout = request.timeout().getMillis();
        this.taskStartedTimeMillis = taskStartTimeMillis;

        this.nodeId = nodeId;
        this.shardIds = shardIds;
    }

    public NodeTaggerRequest(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        string = in.readOptionalString();

        size = in.readVInt();
        timeout = in.readVLong();
        taskStartedTimeMillis = in.readVLong();

        nodeId = in.readString();
        int numShards = in.readVInt();

        shardIds = new HashSet<>(numShards);
        for (int i = 0; i < numShards; i++) {
            shardIds.add(new ShardId(in));
        }
        originalIndices = OriginalIndices.readOriginalIndices(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(field);
        out.writeOptionalString(string);

        out.writeVInt(size);
        // Adjust the amount of permitted time the shard has remaining to gather terms.
        long timeSpentSoFarInCoordinatingNode = System.currentTimeMillis() - taskStartedTimeMillis;
        long remainingTimeForShardToUse = (timeout - timeSpentSoFarInCoordinatingNode);
        //TODO
        remainingTimeForShardToUse = 200000;
        // TODO - if already timed out can we shortcut the trip somehow? Throw exception if remaining time < 0?
        out.writeVLong(remainingTimeForShardToUse);
        out.writeVLong(taskStartedTimeMillis);

        out.writeString(nodeId);
        out.writeVInt(shardIds.size());
        for (ShardId shardId : shardIds) {
            shardId.writeTo(out);
        }
        OriginalIndices.writeOriginalIndices(originalIndices, out);
    }

    public String field() {
        return field;
    }

    @Nullable
    public String string() {
        return string;
    }

    public long taskStartedTimeMillis() {
        return this.taskStartedTimeMillis;
    }

    long nodeStartedTimeMillis() {
        if (nodeStartedTimeMillis == 0) {
            nodeStartedTimeMillis = System.currentTimeMillis();
        }
        return this.nodeStartedTimeMillis;
    }

    public void startTimerOnDataNode() {
        nodeStartedTimeMillis = System.currentTimeMillis();
    }

    public Set<ShardId> shardIds() {
        return Collections.unmodifiableSet(shardIds);
    }

    public int size() {
        return size;
    }

    public long timeout() {
        return timeout;
    }

    public String nodeId() {
        return nodeId;
    }

    public String[] indices() {
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        return originalIndices.indicesOptions();
    }

    public boolean remove(ShardId shardId) {
        return shardIds.remove(shardId);
    }
}
