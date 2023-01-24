/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.solrtexttagger;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.http.util.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 2, numDataNodes = 2)
public class SolrTextTaggerPluginIT extends OpenSearchIntegTestCase
{

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(SolrTextTaggerPlugin.class);
    }

    public void testPluginInstalled() throws IOException, InterruptedException, ExecutionException {
        String index = "foo";

        XContentBuilder builder = jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("content").field("type", "text").endObject()
                    .startObject("title").field("type", "text").endObject()
                .endObject().endObject();
        assertAcked(client().admin().indices().prepareCreate(index).setMapping(builder));

        {
            final String source = "{\"title\":\"" + "Добыча нефти методом горизонтального бурения" + "\", " +
                    "\"content\":\"Разведка сланцевого газоконденсата\"}";
            indexRandom(true, client().prepareIndex(index)/*.setId("1")*/.setSource(source, XContentType.JSON));
        }
        {
            final String source = "{\"title\":\"" + "Гидравлика колбы механика добыча" + "\", " +
                    "\"content\":\"Опрессовка перевозки вагон\"}";
            indexRandom(true, client().prepareIndex(index)/*.setId("1")*/.setSource(source, XContentType.JSON));
        }
        TaggerRequest r = new TaggerRequest(index);
        r.field("title");
        r.string("нефти добыча мазут");
        TaggerResponse rsp = cluster().client().execute(TaggerAction.INSTANCE,r).get();

        assertThat(rsp.getTags().get(0).getMatchText(),containsString("добыча"));

        final Request tag = new Request("GET", index +"/_solr_text_tagger?error_trace=true");
        tag.setJsonEntity("{\"field\":\"title\",\"string\":\"нефти добыча мазут\"}");
        Response response = createRestClient()
                .performRequest(tag);
        String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);

        logger.info("response body: {}", body);
        assertThat(body, containsString("добыча"));
    }
/*
    @Override
    protected boolean ignoreExternalCluster() {
        return false;
    }*/
}
