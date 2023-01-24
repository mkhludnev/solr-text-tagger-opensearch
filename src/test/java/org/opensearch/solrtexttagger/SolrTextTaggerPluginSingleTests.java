/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.solrtexttagger;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
//@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, minNumDataNodes = 2, numDataNodes = 2)
public class SolrTextTaggerPluginSingleTests extends OpenSearchSingleNodeTestCase
{

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
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
            client().prepareIndex(index)/*.setId("1")*/.setSource(source, XContentType.JSON).get();
        }
        {
            final String source = "{\"title\":\"" + "Гидравлика колбы механика добыча" + "\", " +
                    "\"content\":\"Опрессовка перевозки вагон\"}";
            client().prepareIndex(index)/*.setId("1")*/.setSource(source, XContentType.JSON).get();
            client().admin().indices().prepareRefresh(index).get();
        }
        TaggerRequest r = new TaggerRequest(index);
        r.field("title");
        r.string("нефти добыча мазут");
        TaggerResponse rsp = client().execute(TaggerAction.INSTANCE,r).get();

        assertThat(rsp.getTags().get(0).getMatchText(),containsString("добыча"));
    }
/*
    @Override
    protected boolean ignoreExternalCluster() {
        return false;
    }*/
}
