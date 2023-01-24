package org.opensearch.solrtexttagger;

import com.carrotsearch.hppc.cursors.IntCursor;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRunnable;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchTransportService;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.ImmutableOpenIntMap;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.OpenSearchThreadPoolExecutor;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.index.IndexService;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.indices.IndicesService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchService;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.SearchContextAggregations;
import org.opensearch.search.collapse.CollapseContext;
import org.opensearch.search.dfs.DfsSearchResult;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.StoredFieldsContext;
import org.opensearch.search.fetch.subphase.FetchDocValuesContext;
import org.opensearch.search.fetch.subphase.FetchDocValuesPhase;
import org.opensearch.search.fetch.subphase.FetchFieldsContext;
import org.opensearch.search.fetch.subphase.FetchFieldsPhase;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.search.fetch.subphase.ScriptFieldsContext;
import org.opensearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.ScrollContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.profile.Profilers;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.query.ReduceableSearchResult;
import org.opensearch.search.rescore.RescoreContext;
import org.opensearch.search.sort.SortAndFormats;
import org.opensearch.search.suggest.SuggestionSearchContext;
import org.opensearch.solrtexttagger.impl.Tag;
import org.opensearch.solrtexttagger.impl.TaggerRequestHandler;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.RemoteClusterAware;
import org.opensearch.transport.RemoteClusterService;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Predicate;

import static java.lang.String.format;

public class TransportTaggerAction extends HandledTransportAction<TaggerRequest, TaggerResponse> {

    private final ClusterService clusterService;

    private final TransportService transportService;

    private final RemoteClusterService remoteClusterService;

    private final SearchService searchService;

    private final ScriptService scriptService;

    private final IndicesService indicesService;

    private final IndexNameExpressionResolver indexNameExpressionResolver;

    final String transportShardAction;

    private final String shardExecutor;

    private final Settings settings;

    @Inject
    public TransportTaggerAction(ClusterService clusterService,
                                 SearchService searchService,
                                 SearchTransportService searchTransportService,
                                 TransportService transportService,
                                 IndicesService indicesService,
                                 ScriptService scriptService,
                                 ActionFilters actionFilters, Settings settings,
                                 IndexNameExpressionResolver indexNameExpressionResolver) {
        super(TaggerAction.NAME, transportService, actionFilters, TaggerRequest::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.remoteClusterService = searchTransportService.getRemoteClusterService();
        this.indicesService = indicesService;
        this.searchService = searchService;
        this.scriptService = scriptService;

        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.transportShardAction = actionName + "[s]";
        this.shardExecutor = ThreadPool.Names.SEARCH;
        this.settings = settings;

        transportService.registerRequestHandler(transportShardAction, ThreadPool.Names.SAME, NodeTaggerRequest::new,
                new NodeTransportHandler());
    }

    @Override
    protected void doExecute(Task task, TaggerRequest request, ActionListener<TaggerResponse> actionListener) {
        new AsyncBroadcastAction(task, request, actionListener).start();
    }

    protected NodeTaggerRequest newNodeRequest(final OriginalIndices originalIndices,
                                               final String nodeId,
                                               final Set<ShardId> shardIds,
                                               TaggerRequest request,
                                               long taskStartMillis
    ) {
        return new NodeTaggerRequest(originalIndices, nodeId, shardIds, request, taskStartMillis);
    }

    protected NodeTaggerResponse readShardResponse(StreamInput in) throws IOException {
        return new NodeTaggerResponse(in);
    }

    protected Map<String, Set<ShardId>> getNodeBundles(ClusterState clusterState, String[] concreteIndices) {
        // Group targeted shards by nodeId
        Map<String, Set<ShardId>> fastNodeBundles = new HashMap<>();
        for (String indexName : concreteIndices) {

            String[] singleIndex = { indexName };

            GroupShardsIterator<ShardIterator> shards = clusterService.operationRouting()
                    .searchShards(clusterState, singleIndex, null, null);

            for (ShardIterator copiesOfShard : shards) {
                ShardRouting selectedCopyOfShard = null;
                for (ShardRouting copy : copiesOfShard) {
                    // Pick the first active node with a copy of the shard
                    if (copy.active() && copy.assignedToNode()) {
                        selectedCopyOfShard = copy;
                        break;
                    }
                }
                if (selectedCopyOfShard == null) {
                    break;
                }
                String nodeId = selectedCopyOfShard.currentNodeId();
                final Set<ShardId> bundle;
                if (fastNodeBundles.containsKey(nodeId)) {
                    bundle = fastNodeBundles.get(nodeId);
                } else {
                    bundle = new HashSet<>();
                    fastNodeBundles.put(nodeId, bundle);
                }
                if (bundle != null) {
                    bundle.add(selectedCopyOfShard.shardId());
                }
            }
        }
        return fastNodeBundles;
    }


    protected ClusterBlockException checkGlobalBlock(ClusterState state, TaggerRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    protected ClusterBlockException checkRequestBlock(ClusterState state, TaggerRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }

    protected TaggerResponse mergeResponses(
            TaggerRequest request,
            AtomicReferenceArray<?> atomicResponses,
            boolean complete,
            Map<String, Set<ShardId>> nodeBundles
    ) {
        int successfulShards = 0;
        int failedShards = 0;
        List<DefaultShardOperationFailedException> shardFailures = null;
        List<List<Tag>> termsList = new ArrayList<>();
        for (int i = 0; i < atomicResponses.length(); i++) {
            Object atomicResponse = atomicResponses.get(i);
            if (atomicResponse == null) {
                // simply ignore non active operations
            } else if (atomicResponse instanceof NodeTaggerResponse) {
                NodeTaggerResponse str = (NodeTaggerResponse) atomicResponse;
                // Only one node response has to be incomplete for the entire result to be labelled incomplete.
                if (str.isComplete() == false) {
                    complete = false;
                }

                Set<ShardId> shards = nodeBundles.get(str.getNodeId());
                if (str.getError() != null) {
                    complete = false;
                    // A single reported error is assumed to be for all shards queried on that node.
                    // When reading we read from multiple Lucene indices in one unified view so any error is
                    // assumed to be all shards on that node.
                    failedShards += shards.size();
                    if (shardFailures == null) {
                        shardFailures = new ArrayList<>();
                    }
                    for (ShardId failedShard : shards) {
                        shardFailures.add(
                                new DefaultShardOperationFailedException(
                                        new BroadcastShardOperationFailedException(failedShard, str.getError())
                                )
                        );
                    }
                } else {
                    successfulShards += shards.size();
                }
                termsList.add(str.tags());
            } else if (atomicResponse instanceof RemoteClusterTaggerResponse) {
                RemoteClusterTaggerResponse rc = (RemoteClusterTaggerResponse) atomicResponse;
                // Only one node response has to be incomplete for the entire result to be labelled incomplete.
                if (rc.resp.isComplete() == false || rc.resp.getFailedShards() > 0) {
                    complete = false;
                }
                successfulShards += rc.resp.getSuccessfulShards();
                failedShards += rc.resp.getFailedShards();
                for (DefaultShardOperationFailedException exc : rc.resp.getShardFailures()) {
                    if (shardFailures == null) {
                        shardFailures = new ArrayList<>();
                    }
                    shardFailures.add(
                            new DefaultShardOperationFailedException(rc.clusterAlias + ":" + exc.index(), exc.shardId(), exc.getCause())
                    );
                }
                //@TODO docs
                termsList.add(rc.resp.getTags());
            } else {
                throw new AssertionError("Unknown atomic response type: " + atomicResponse.getClass().getName());
            }
        }

        List<Tag> ans = termsList.size() == 1 ? termsList.get(0) : mergeResponses(termsList, request.size());
        // @TODO
        List<String>  docs = List.of("TODO", "FIXME");
        return new TaggerResponse(ans, docs,failedShards + successfulShards, successfulShards, failedShards, shardFailures, complete);
    }

    private List<Tag> mergeResponses(List<List<Tag>> termsList, int size) {
        final ArrayList<Tag> strings = new ArrayList<>();
        for (List<Tag> l : termsList) {
            strings.addAll(l);
        }
        return strings;
    }

    protected NodeTaggerResponse dataNodeOperation(NodeTaggerRequest request, Task task) throws IOException {
        List<Tag> termsList = new ArrayList<>();
        String error = null;

        long timeout_millis = request.timeout();
        long scheduledEnd = request.nodeStartedTimeMillis() + timeout_millis;

        ArrayList<Closeable> openedResources = new ArrayList<>();
        try {
            for (ShardId shardId : request.shardIds()) {
                // Check we haven't just arrived on a node and time is up already.
                // TODO time check
                if (false && System.currentTimeMillis() > scheduledEnd) {
                    return new NodeTaggerResponse(request.nodeId(), termsList,Collections.emptyList(), error, false);
                }
                final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
                final IndexShard indexShard = indexService.getShard(shardId.getId());
                final SearchContext searchContext = searchService.createSearchContext(new ShardSearchRequest(shardId, request.timeout(), AliasFilter.EMPTY), SearchService.NO_TIMEOUT);
                searchContext.setTask(new SearchShardTask(task.getId(), task.getType(),
                        task.getAction(), task.getDescription(), task.getParentTaskId(),Map.of()));
                //Engine.Searcher searcher = indexShard.acquireSearcher(Engine.SEARCH_SOURCE);
                openedResources.add(searchContext);
                //final QueryShardContext queryShardContext = indexService.newQueryShardContext(shardId.id(), searcher, request::nodeStartedTimeMillis, null);
                final MappedFieldType mappedFieldType = indexShard.mapperService().fieldType(request.field());
                final NamedAnalyzer searchAnalyzer = mappedFieldType.getTextSearchInfo().getSearchAnalyzer();
                final Terms terms = MultiTerms.getTerms(searchContext.searcher().getTopReaderContext().reader(), request.field());

                final TaggerRequestHandler taggerRequestHandler = new TaggerRequestHandler() {
                    @Override
                    protected void fetch(ImmutableOpenIntMap<Tag> docnums) {
                        final int[] ints = new int[docnums.size()];
                        int i=0;
                        for (IntCursor c : docnums.keys()) {
                            ints[i++]=c.value;
                        }
                        final FetchPhase fetchPhase = new FetchPhase(List.of(new FetchFieldsPhase()));

                        //final SearchContext searchContext = createSearchContext(queryShardContext, indexShard.mapperService(), indexService);
                        searchContext.docIdsToLoad(ints,0,ints.length);
                        searchContext.fetchFieldsContext(new FetchFieldsContext(
                                List.of())); //new FieldAndFormat(GetResult._ID, null)
                        fetchPhase.execute(searchContext);
                        // just mutate mapped tags
                        for (SearchHit hit : searchContext.fetchResult().hits().getHits()) {
                            docnums.get(hit.docId()).addId(hit.getId());
                        }
                    }
                };
                List<Tag> tags = taggerRequestHandler.handleRequestBody(request.field(), request.string(), searchAnalyzer.analyzer(), terms);
                // per collocated shard
                termsList.addAll(tags);
            }
            if (termsList.size() == 0) {
                // No term enums available
                return new NodeTaggerResponse(request.nodeId(), termsList, Collections.emptyList(), error, true);
            }
            //MultiShardTags te = new MultiShardTags(shardTermsEnums.toArray(new TermsEnum[0]));

            int shard_size = request.size();
            // All the above prep might take a while - do a timer check now before we continue further.
            if (System.currentTimeMillis() > scheduledEnd) {
                return new NodeTaggerResponse(request.nodeId(), termsList, Collections.emptyList(), error, false);
            }
        } catch (Exception e) {
            error = ExceptionsHelper.stackTrace(e);
        } finally {
            IOUtils.close(openedResources);
        }
        // @TODO
        final List<String> docsTODO = Collections.<String>emptyList();
        return new NodeTaggerResponse(request.nodeId(), termsList, docsTODO, error, true);
    }

    // @TODO
    private boolean canAccess(
            ShardId shardId,
            NodeTaggerRequest request,
            ThreadContext threadContext
    ) {
        return true;
    }
    // @TODO
    private boolean canMatchShard(ShardId shardId, NodeTaggerRequest req) throws IOException {
        return true;
    }


    protected class AsyncBroadcastAction {

        private final Task task;
        private final TaggerRequest request;
        private ActionListener<TaggerResponse> listener;
        private final DiscoveryNodes nodes;
        private final int expectedOps;
        private final AtomicInteger counterOps = new AtomicInteger();
        private final AtomicReferenceArray<Object> atomicResponses;
        private final Map<String, Set<ShardId>> nodeBundles;
        private final OriginalIndices localIndices;
        private final Map<String, OriginalIndices> remoteClusterIndices;

        protected AsyncBroadcastAction(Task task, TaggerRequest request, ActionListener<TaggerResponse> listener) {
            this.task = task;
            this.request = request;
            this.listener = listener;

            ClusterState clusterState = clusterService.state();

            ClusterBlockException blockException = checkGlobalBlock(clusterState, request);
            if (blockException != null) {
                throw blockException;
            }

            //@TODO
            Predicate<String> todo = (s)->true;
            this.remoteClusterIndices = remoteClusterService.groupIndices(request.indicesOptions(), request.indices(), todo);
            this.localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);

            // update to concrete indices
            String[] concreteIndices = localIndices == null
                    ? new String[0]
                    : indexNameExpressionResolver.concreteIndexNames(clusterState, localIndices);
            blockException = checkRequestBlock(clusterState, request, concreteIndices);
            if (blockException != null) {
                throw blockException;
            }

            nodes = clusterState.nodes();
            logger.trace("resolving shards based on cluster state version [{}]", clusterState.version());
            nodeBundles = getNodeBundles(clusterState, concreteIndices);
            expectedOps = nodeBundles.size() + remoteClusterIndices.size();

            atomicResponses = new AtomicReferenceArray<>(expectedOps);
        }

        public void start() {
            if (expectedOps == 0) {
                // no shards
                try {
                    listener.onResponse(mergeResponses(request, new AtomicReferenceArray<>(0), true, nodeBundles));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
                // TODO or remove above try and instead just call finishHim() here? Helps keep return logic consistent
                return;
            }
            // count the local operations, and perform the non local ones
            int numOps = 0;
            for (final String nodeId : nodeBundles.keySet()) {
                if (checkForEarlyFinish()) {
                    return;
                }
                Set<ShardId> shardIds = nodeBundles.get(nodeId);
                if (shardIds.size() > 0) {
                    performOperation(nodeId, shardIds, numOps);
                } else {
                    // really, no shards active in this group
                    onNodeFailure(nodeId, numOps, null);
                }
                ++numOps;
            }
            // handle remote clusters
            for (String clusterAlias : remoteClusterIndices.keySet()) {
                performRemoteClusterOperation(clusterAlias, remoteClusterIndices.get(clusterAlias), numOps);
                ++numOps;
            }
        }

        // Returns true if we exited with a response to the caller.
        boolean checkForEarlyFinish() {
            long now = System.currentTimeMillis();
            // TODO
            if (false && (now - task.getStartTime()) > request.timeout().getMillis()) {
                finishHim(false);
                return true;
            }
            return false;
        }

        protected void performOperation(final String nodeId, final Set<ShardId> shardIds, final int opsIndex) {
            if (shardIds.size() == 0) {
                // no more active shards... (we should not really get here, just safety)
                onNodeFailure(nodeId, opsIndex, null);
            } else {
                try {
                    final NodeTaggerRequest nodeRequest = newNodeRequest(localIndices, nodeId, shardIds, request, task.getStartTime());
                    nodeRequest.setParentTask(clusterService.localNode().getId(), task.getId());
                    DiscoveryNode node = nodes.get(nodeId);
                    if (node == null) {
                        // no node connected, act as failure
                        onNodeFailure(nodeId, opsIndex, null);
                    } else if (checkForEarlyFinish() == false) {
                        transportService.sendRequest(
                                node,
                                transportShardAction,
                                nodeRequest,
                                new TransportResponseHandler<NodeTaggerResponse>() {
                                    @Override
                                    public NodeTaggerResponse read(StreamInput in) throws IOException {
                                        return readShardResponse(in);
                                    }

                                    @Override
                                    public void handleResponse(NodeTaggerResponse response) {
                                        onNodeResponse(nodeId, opsIndex, response);
                                    }

                                    @Override
                                    public void handleException(TransportException exc) {
                                        onNodeFailure(nodeId, opsIndex, exc);
                                    }

                                    @Override
                                    public String executor() {
                                        return ThreadPool.Names.SAME;
                                    }
                                }
                        );
                    }
                } catch (Exception exc) {
                    onNodeFailure(nodeId, opsIndex, exc);
                }
            }
        }

        void performRemoteClusterOperation(final String clusterAlias, final OriginalIndices remoteIndices, final int opsIndex) {
            try {
                TaggerRequest req = new TaggerRequest(request).indices(remoteIndices.indices());

                Client remoteClient = remoteClusterService.getRemoteClusterClient(transportService.getThreadPool(), clusterAlias);
                remoteClient.execute(TaggerAction.INSTANCE, req, new ActionListener<>() {
                    @Override
                    public void onResponse(TaggerResponse termsEnumResponse) {
                        onRemoteClusterResponse(
                                clusterAlias,
                                opsIndex,
                                new RemoteClusterTaggerResponse(clusterAlias, termsEnumResponse)
                        );
                    }

                    @Override
                    public void onFailure(Exception exc) {
                        onRemoteClusterFailure(clusterAlias, opsIndex, exc);
                    }
                });
            } catch (Exception exc) {
                onRemoteClusterFailure(clusterAlias, opsIndex, null);
            }
        }

        private void onNodeResponse(String nodeId, int opsIndex, NodeTaggerResponse response) {
            logger.trace("received response for node {}", nodeId);
            atomicResponses.set(opsIndex, response);
            if (expectedOps == counterOps.incrementAndGet()) {
                finishHim(true);
            } else {
                checkForEarlyFinish();
            }
        }

        private void onRemoteClusterResponse(String clusterAlias, int opsIndex, RemoteClusterTaggerResponse response) {
            logger.trace("received response for cluster {}", clusterAlias);
            atomicResponses.set(opsIndex, response);
            if (expectedOps == counterOps.incrementAndGet()) {
                finishHim(true);
            } else {
                checkForEarlyFinish();
            }
        }

        private void onNodeFailure(String nodeId, int opsIndex, Exception exc) {
            logger.trace("received failure {} for node {}", exc, nodeId);
            // TODO: Handle exceptions in the atomic response array
            if (expectedOps == counterOps.incrementAndGet()) {
                finishHim(true);
            }
        }

        private void onRemoteClusterFailure(String clusterAlias, int opsIndex, Exception exc) {
            logger.trace("received failure {} for cluster {}", exc, clusterAlias);
            // TODO: Handle exceptions in the atomic response array
            if (expectedOps == counterOps.incrementAndGet()) {
                finishHim(true);
            }
        }

        // Can be called multiple times - either for early time-outs or for fully-completed collections.
        protected synchronized void finishHim(boolean complete) {
            if (listener == null) {
                return;
            }
            try {
                listener.onResponse(mergeResponses(request, atomicResponses, complete, nodeBundles));
            } catch (Exception e) {
                listener.onFailure(e);
            } finally {
                listener = null;
            }
        }
    }

    class NodeTransportHandler implements TransportRequestHandler<NodeTaggerRequest> {

        @Override
        public void messageReceived(NodeTaggerRequest request, TransportChannel channel, Task task) throws Exception {
            asyncNodeOperation(request, task, ActionListener.wrap(channel::sendResponse, e -> {
                try {
                    channel.sendResponse(e);
                } catch (Exception e1) {
                    logger.warn(() -> format("Failed to send error response for action [%s] and request [%s]", actionName, request), e1);
                }
            }));
        }
    }

    private void asyncNodeOperation(NodeTaggerRequest request, Task task, ActionListener<NodeTaggerResponse> listener)
            throws IOException {
        // Start the clock ticking on the data node using the data node's local current time.
        request.startTimerOnDataNode();

        // DLS/FLS check copied from ResizeRequestInterceptor - check permissions and
        // any index_filter canMatch checks on network thread before allocating work
        ThreadContext threadContext = transportService.getThreadPool().getThreadContext();

        for (ShardId shardId : request.shardIds().toArray(new ShardId[0])) {
            if (canAccess(shardId, request,  threadContext) == false || canMatchShard(shardId, request) == false) {
                // Permission denied or can't match, remove shardID from request
                request.remove(shardId);
            }
        }
        if (request.shardIds().size() == 0) {
            listener.onResponse(new NodeTaggerResponse(request.nodeId(), Collections.emptyList(), Collections.emptyList(), null, true));
        } else {
            // Use the search threadpool if its queue is empty
            assert transportService.getThreadPool().executor(ThreadPool.Names.SEARCH) instanceof OpenSearchThreadPoolExecutor
                    : "SEARCH threadpool must be an instance of ThreadPoolExecutor";
            OpenSearchThreadPoolExecutor ex = (OpenSearchThreadPoolExecutor) transportService.getThreadPool().executor(ThreadPool.Names.SEARCH);
            final String executorName = ex.getQueue().size() == 0 ? ThreadPool.Names.SEARCH : shardExecutor;
            transportService.getThreadPool()
                    .executor(executorName)
                    .execute(ActionRunnable.supply(listener, () -> dataNodeOperation(request, task)));
        }
    }

    private static class RemoteClusterTaggerResponse {
        final String clusterAlias;
        final TaggerResponse resp;

        private RemoteClusterTaggerResponse(String clusterAlias, TaggerResponse resp) {
            this.clusterAlias = clusterAlias;
            this.resp = resp;
        }
    }

    private static class TermIterator implements Iterator<String>, Comparable<TermIterator> {
        private final Iterator<String> iterator;
        private String current;

        private TermIterator(Iterator<String> iterator) {
            this.iterator = iterator;
            this.current = iterator.next();
        }

        public String term() {
            return current;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public String next() {
            return current = iterator.next();
        }

        @Override
        public int compareTo(TermIterator o) {
            return current.compareTo(o.term());
        }
    }
}
