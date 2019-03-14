/*
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
package de.ubleipzig.kafka.consumer;

import java.io.File;
import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.junit.jupiter.Container;


@SpringBootTest
public class IntegrationTest {
    private static final Integer ELASTICSEARCH_HOST_PORT = 9200;

    @Autowired
    KafkaCamelConsumerApplication consumer;

    @Container
    public static DockerComposeContainer environment =
            new DockerComposeContainer(new File("src/test/resources/compose-test.yml"));

    @Before
    public void init() {
        environment.start();
    }

    @Ignore
    @Test
    public void testAll() {
        KafkaCamelConsumerApplication.main(new String[] {});
        try {
            final HttpHost host = new HttpHost("localhost", ELASTICSEARCH_HOST_PORT, null);
            RestClient restClient = RestClient.builder(host)
                    .build();
            Request request = new Request("GET", "/");
            Response response = restClient.performRequest(request);
            HttpEntity resEntity = response.getEntity();
            final String body = EntityUtils.toString(resEntity);
            System.out.println(body);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
