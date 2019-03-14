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

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "elasticsearch")
public class ElasticSearchProperties {
    private String host;
    private Integer port;
    private String scheme;
    private String eventIndexName;
    private String eventIndexType;
    private String docIndexName;
    private String docIndexType;
    private String indexableTypes;

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public String getScheme() {
        return scheme;
    }

    public String getEventIndexName() {
        return eventIndexName;
    }

    public String getEventIndexType() {
        return eventIndexType;
    }

    public String getDocIndexName() {
        return docIndexName;
    }

    public String getDocIndexType() {
        return docIndexType;
    }

    public String getIndexableTypes() {
        return indexableTypes;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    public void setEventIndexName(String eventIndexName) {
        this.eventIndexName = eventIndexName;
    }

    public void setEventIndexType(String eventIndexType) {
        this.eventIndexType = eventIndexType;
    }

    public void setDocIndexName(String docIndexName) {
        this.docIndexName = docIndexName;
    }

    public void setDocIndexType(String docIndexType) {
        this.docIndexType = docIndexType;
    }

    public void setIndexableTypes(String indexableTypes) {
        this.indexableTypes = indexableTypes;
    }
}
