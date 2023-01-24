/*
 * This software was produced for the U. S. Government
 * under Contract No. W15P7T-11-C-F600, and is
 * subject to the Rights in Noncommercial Computer Software
 * and Noncommercial Computer Software Documentation
 * Clause 252.227-7014 (JUN 1995)
 *
 * Copyright 2013 The MITRE Corporation. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opensearch.solrtexttagger.impl;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Terms;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.IntsRef;
import org.opensearch.common.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.collect.ImmutableOpenIntMap;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Scans posted text, looking for matching strings in the Solr index. The public static final String
 * members are request parameters. This handler is also called the "SolrTextTagger".
 *
 * @since 7.4.0
 */
public abstract class TaggerRequestHandler {

  /** Request parameter. */
  public static final String OVERLAPS = "overlaps";
  /** Request parameter. */
  public static final String TAGS_LIMIT = "tagsLimit";
  /** Request parameter. */
  public static final String MATCH_TEXT = "matchText";
  /** Request parameter. */
  public static final String SKIP_ALT_TOKENS = "skipAltTokens";
  /** Request parameter. */
  public static final String IGNORE_STOPWORDS = "ignoreStopwords";
  /** Request parameter. */
  public static final String XML_OFFSET_ADJUST = "xmlOffsetAdjust";

  private static final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());

  public List<Tag> handleRequestBody(String indexedField, String text, Analyzer analyzer, Terms terms) throws Exception {

    // --Read params
    //final String indexedField = req.field();
    if (indexedField == null) throw new IllegalArgumentException("required param 'field'");

    final TagClusterReducer tagClusterReducer =
        chooseTagClusterReducer("ALL");//req.getParams().get(OVERLAPS));
    final int rows = 100;//req.size();//req.getParams().getInt(CommonParams.ROWS, 10000);
    final int tagsLimit = 100;//req.getParams().getInt(TAGS_LIMIT, 1000);
    final boolean addMatchText = true;//req.getParams().getBool(MATCH_TEXT, false);
    /*final SchemaField idSchemaField = req.getSchema().getUniqueKeyField();
    if (idSchemaField == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "The tagger requires a" + "uniqueKey in the schema."); // TODO this could be relaxed
    }*/
    final boolean skipAltTokens = false;//req.getParams().getBool(SKIP_ALT_TOKENS, false);
    final boolean ignoreStopWords = false;
        //req.getParams().getBool(IGNORE_STOPWORDS, fieldHasIndexedStopFilter(indexedField, req));

    // --Get posted data
    if (Strings.isEmpty(text)) {
      throw new IllegalArgumentException(" requires text to be POSTed to it");
    }

    // We may or may not need to read the input into a string
//    final InputStringLazy inputStringFuture = new InputStringLazy(new StringReader(req.string()));

    final OffsetCorrector offsetCorrector = null;//getOffsetCorrector(req.getParams(), inputStringFuture);

    //final SolrIndexSearcher searcher = req.getSearcher();
    //final FixedBitSet matchDocIdsBS = new FixedBitSet(searcher.maxDoc());
    //final List<SimpleOrderedMap<?>> tags = new ArrayList<>(2000);
    List<Tag> tags = new ArrayList<>(1000);
    try {
      //Analyzer analyzer = req.getSchema().getField(indexedField).getType().getQueryAnalyzer();
      try (TokenStream tokenStream = analyzer.tokenStream(indexedField, text//inputReader
      )) {
        //Terms terms = searcher.getSlowAtomicReader().terms(indexedField);
        if (terms != null) {
          final ImmutableOpenIntMap.Builder<Tag> map = ImmutableOpenIntMap.<Tag>builder();
          BiConsumer<Tag, IntsRef> tagToDocNums = (t,dnums) -> {
            for (int i = dnums.offset; i< dnums.length + dnums.offset; i++) {
              final int docNum = dnums.ints[i];
              if (!map.containsKey(docNum)) {
                map.put(docNum, t);
              }
            }
            tags.add(t);
          };
          Tagger tagger =
              new Tagger(
                  terms,
                  null,//computeDocCorpus(req),
                  tokenStream,
                  tagClusterReducer,
                  skipAltTokens,
                  ignoreStopWords) {
                @Override
                protected void tagCallback(int startOffset, int endOffset, Object docIdsKey) {
                  if (tags.size() >= tagsLimit) return;
                  if (offsetCorrector != null) {
                    int[] offsetPair = offsetCorrector.correctPair(startOffset, endOffset);
                    if (offsetPair == null) {
                      log.debug(
                          "Discarded offsets [{}, {}] because couldn't balance XML.",
                          startOffset,
                          endOffset);
                      return;
                    }
                    startOffset = offsetPair[0];
                    endOffset = offsetPair[1];
                  }
                  Tag tag = new Tag(startOffset, endOffset);
                  if (addMatchText) {
                    tag.setMatchText(text.substring(startOffset, endOffset));
                  }
                  tagToDocNums.accept(tag, (IntsRef) docIdsKey);
                  // below caches, and also flags matchDocIdsBS
                  //tag.add("ids", lookupSchemaDocIds(docIdsKey));
                }
/*
                Map<Object, List<Object>> docIdsListCache = new HashMap<>(2000);

                ValueSourceAccessor uniqueKeyCache =
                    new ValueSourceAccessor(
                        searcher, idSchemaField.getType().getValueSource(idSchemaField, null));
// it's kind of org.elasticsearch.search.fetch.FetchPhase#buildSearchHits
                private List<Object> lookupSchemaDocIds(Object docIdsKey) {
                  List<Object> schemaDocIds = docIdsListCache.get(docIdsKey);
                  if (schemaDocIds != null) return schemaDocIds;
                  IntsRef docIds = lookupDocIds(docIdsKey);
                  // translate lucene docIds to schema ids
                  schemaDocIds = new ArrayList<>(docIds.length);
                  for (int i = docIds.offset; i < docIds.offset + docIds.length; i++) {
                    int docId = docIds.ints[i];
                    assert i == docIds.offset || docIds.ints[i - 1] < docId : "not sorted?";
                    matchDocIdsBS.set(docId); // also, flip docid in bitset
                    try {
                      schemaDocIds.add(uniqueKeyCache.objectVal(docId)); // translates here
                    } catch (IOException e) {
                      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
                    }
                  }
                  assert !schemaDocIds.isEmpty();

                  docIdsListCache.put(docIds, schemaDocIds);
                  return schemaDocIds;
                }
*/
              };
          tagger.enableDocIdsCache(2000); // TODO configurable
          tagger.process();
          fetch(map.build());
        }
      }
    } finally {
      //inputReader.close();
    }
    //rsp.add("tagsCount", tags.size());
    //rsp.add("tags", tags);

    //rsp.setReturnFields(new SolrReturnFields(req));

    // Solr's standard name for matching docs in response
    //rsp.add("response", getDocList(rows, matchDocIdsBS));
    return tags;
  }

  protected abstract void fetch(ImmutableOpenIntMap<Tag> build) ;

/*
  private DocList getDocList(int rows, FixedBitSet matchDocIdsBS) throws IOException {
    // Now we must supply a Solr DocList and add it to the response.
    //  Typically this is gotten via a SolrIndexSearcher.search(), but in this case we
    //  know exactly what documents to return, the order doesn't matter nor does
    //  scoring.
    //  Ideally an implementation of DocList could be directly implemented off
    //  of a BitSet, but there are way too many methods to implement for a minor
    //  payoff.
    int matchDocs = matchDocIdsBS.cardinality();
    int[] docIds = new int[Math.min(rows, matchDocs)];
    DocIdSetIterator docIdIter = new BitSetIterator(matchDocIdsBS, 1);
    for (int i = 0; i < docIds.length; i++) {
      docIds[i] = docIdIter.nextDoc();
    }
    return new DocSlice(0, docIds.length, docIds, null, matchDocs, 1f, TotalHits.Relation.EQUAL_TO);
  }
*/

  private TagClusterReducer chooseTagClusterReducer(String overlaps) {
    TagClusterReducer tagClusterReducer;
    if (overlaps == null || overlaps.equals("NO_SUB")) {
      tagClusterReducer = TagClusterReducer.NO_SUB;
    } else if (overlaps.equals("ALL")) {
      tagClusterReducer = TagClusterReducer.ALL;
    } else if (overlaps.equals("LONGEST_DOMINANT_RIGHT")) {
      tagClusterReducer = TagClusterReducer.LONGEST_DOMINANT_RIGHT;
    } else {
      throw new IllegalArgumentException(
           "unknown tag overlap mode: " + overlaps);
    }
    return tagClusterReducer;
  }

  /** See LUCENE-4541 or org.apache.solr.response.transform.ValueSourceAugmenter}. */
  static class ValueSourceAccessor {
    private final List<LeafReaderContext> readerContexts;
    private final ValueSource valueSource;
    private final Map<Object, Object> fContext;
    private final FunctionValues[] functionValuesPerSeg;
    private final int[] functionValuesDocIdPerSeg;

    ValueSourceAccessor(IndexSearcher searcher, ValueSource valueSource) {
      readerContexts = searcher.getIndexReader().leaves();
      this.valueSource = valueSource;
      fContext = ValueSource.newContext(searcher);
      functionValuesPerSeg = new FunctionValues[readerContexts.size()];
      functionValuesDocIdPerSeg = new int[readerContexts.size()];
    }

    Object objectVal(int topDocId) throws IOException {
      // lookup segment level stuff:
      int segIdx = ReaderUtil.subIndex(topDocId, readerContexts);
      LeafReaderContext rcontext = readerContexts.get(segIdx);
      int segDocId = topDocId - rcontext.docBase;
      // unfortunately Lucene 7.0 requires forward only traversal (with no reset method).
      //   So we need to track our last docId (per segment) and re-fetch the FunctionValues. :-(
      FunctionValues functionValues = functionValuesPerSeg[segIdx];
      if (functionValues == null || segDocId < functionValuesDocIdPerSeg[segIdx]) {
        functionValues = functionValuesPerSeg[segIdx] = valueSource.getValues(fContext, rcontext);
      }
      functionValuesDocIdPerSeg[segIdx] = segDocId;

      // get value:
      return functionValues.objectVal(segDocId);
    }
  }
}
