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
package de.ubleipzig.kafka.consumer.camel.elasticsearch;

import static de.ubleipzig.kafka.consumer.camel.elasticsearch.ElasticsearchHighLevelClientImpl.getDocumentId;
import static de.ubleipzig.kafka.consumer.camel.elasticsearch.ProcessorUtils.tokenizePropertyPlaceholder;
import static java.util.stream.Collectors.toList;
import static org.apache.camel.Exchange.HTTP_METHOD;
import static org.apache.camel.Exchange.HTTP_URI;
import static org.apache.camel.LoggingLevel.INFO;
import static org.apache.camel.builder.PredicateBuilder.and;
import static org.apache.camel.builder.PredicateBuilder.in;
import static org.trellisldp.camel.ActivityStreamProcessor.ACTIVITY_STREAM_OBJECT_ID;
import static org.trellisldp.camel.ActivityStreamProcessor.ACTIVITY_STREAM_OBJECT_TYPE;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.trellisldp.camel.ActivityStreamProcessor;

@Component
public class CamelRouterComponent extends RouteBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(CamelRouterComponent.class);

    private static final String HTTP_ACCEPT = "Accept";
    private RestHighLevelClient client;
    private String eventIndexName;
    private String eventIndexType;
    private String docIndexName;
    private String docIndexType;
    private String indexableTypes;

    @Autowired
    public CamelRouterComponent(ElasticSearchProperties esProperties) {
        String host = esProperties.getHost();
        Integer port = esProperties.getPort();
        String scheme = esProperties.getScheme();
        HttpHost elasticSearchHost = new HttpHost(host, port, scheme);
        this.client = new RestHighLevelClient(RestClient.builder(elasticSearchHost));
        this.eventIndexName = esProperties.getEventIndexName();
        this.eventIndexType = esProperties.getEventIndexType();
        this.docIndexName = esProperties.getDocIndexName();
        this.docIndexType = esProperties.getDocIndexType();
        this.indexableTypes = esProperties.getIndexableTypes();
    }

    @Override
    public void configure() throws Exception {
        LOGGER.info("About to start route: Kafka Server -> Log ");
        CamelContext context = new DefaultCamelContext();

        from("kafka:{{consumer.topic}}?brokers={{kafka.host}}:{{kafka.port}}"
                + "&maxPollRecords={{consumer.maxPollRecords}}"
                + "&consumersCount={{consumer.consumersCount}}"
                + "&seekTo={{consumer.seekTo}}"
                + "&groupId={{consumer.group}}")
                .routeId("FromKafka")
                .unmarshal()
                .json(JsonLibrary.Jackson)
                .process(new ActivityStreamProcessor())
                .marshal()
                .json(JsonLibrary.Jackson, true)
                .log(INFO, LOGGER, "Serializing KafkaProducerMessage to JSON")
                .setHeader("event.index.name", constant(this.eventIndexName))
                .setHeader("event.index.type", constant(this.eventIndexType))
                .setHeader("document.index.name", constant(this.docIndexName))
                .setHeader("document.index.type", constant(this.docIndexType))
                //.to("file://{{serialization.log}}");
                .to("direct:get");
        from("direct:get").routeId("DocumentGet")
                .choice()
                .when(and(in(tokenizePropertyPlaceholder(getContext(), this.indexableTypes, ",").stream()
                        .map(type -> header(ACTIVITY_STREAM_OBJECT_TYPE).contains(type))
                        .collect(toList()))))
                .setHeader(HTTP_METHOD)
                .constant("GET")
                .setHeader(HTTP_URI)
                .header(ACTIVITY_STREAM_OBJECT_ID)
                .setHeader(HTTP_ACCEPT)
                .constant("application/ld+json")
                .convertBodyTo(String.class)
                .process(exchange -> {
                    final String jsonString = exchange.getIn()
                            .getBody(String.class);
                    LOGGER.debug("Getting Document Resource {}", jsonString);
                })
                .to("direct:docIndex")
                .otherwise()
                .to("direct:eventIndex");
        from("direct:docIndex").routeId("DocIndex")
                .process(exchange -> {
                    final String docId = getDocumentId();
                    final IndexRequest request = new IndexRequest(exchange.getIn()
                            .getHeader("document.index.name")
                            .toString(), exchange.getIn()
                            .getHeader("document.index.type")
                            .toString(), docId);
                    final String jsonString = exchange.getIn()
                            .getBody(String.class);
                    XContentBuilder builder = XContentFactory.jsonBuilder();
                    XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry
                            .EMPTY, null, jsonString);
                    builder.startObject();
                    builder.field("web-anno");
                    builder.copyCurrentStructure(parser);
                    builder.endObject();
                    LOGGER.debug("Indexing Document Body {}", jsonString);
                    request.source(jsonString, XContentType.JSON);
                    final IndexResponse indexResponse = this.client.index(request, RequestOptions.DEFAULT);
                    LOGGER.info("Document ID {} Indexing Status: {}", docId, indexResponse.status());
                });
        from("direct:eventIndex").routeId("EventIndex")
                .process(exchange -> {
                    final String docId = getDocumentId();
                    final IndexRequest request = new IndexRequest(exchange.getIn()
                            .getHeader("event.index.name")
                            .toString(), exchange.getIn()
                            .getHeader("event.index.type")
                            .toString(), docId);
                    final String jsonString = exchange.getIn()
                            .getBody(String.class);
                    LOGGER.debug("Indexing Event Body {}", jsonString);
                    request.source(jsonString, XContentType.JSON);
                    final IndexResponse indexResponse = this.client.index(request, RequestOptions.DEFAULT);
                    LOGGER.info("Event ID {} Indexing Status: {}", docId, indexResponse.status());
                });
    }
}

