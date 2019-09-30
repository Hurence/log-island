/**
 * Copyright (C) 2016 Hurence (support@hurence.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hurence.logisland.service.solr;

// Author: Simon Kitching
// This code is in the public domain


import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * A JUnit rule which starts an embedded solr instance.
 * <p>
 * Tests which use this rule will run relatively slowly, and should only be used when more conventional unit tests are
 * not sufficient - eg when testing DAO-specific code.
 * </p>
 */
public class SolrRule extends ExternalResource {

    private Logger logger = LoggerFactory.getLogger(SolrRule.class);
    private EmbeddedSolrServer server;
    private CoreContainer container;

    @Override
    protected void before() throws Throwable {

        FileUtils.deleteDirectory(new File("src/integration-test/resources/solr/chronix/data"));
        container = new CoreContainer("src/integration-test/resources/solr/");
        container.load();

        server = new EmbeddedSolrServer(container, "chronix" );

        getClient().deleteByQuery("*:*");
        getClient().commit();
    };

    @Override
    protected void after() {
        try {
            server.close();
        } catch (IOException e) {
            logger.error("error while closing server", e);
        }
    };

    /**
     * Return the object through which operations can be performed on the ES cluster.
     */
    public SolrClient getClient() {
        return server;
    }


}