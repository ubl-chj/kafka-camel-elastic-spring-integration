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
package de.ubleipzig.kafka.producer;

import static de.ubleipzig.kafka.producer.processor.JsonSerializer.serialize;
import static net.andreinc.mockneat.unit.address.Countries.countries;
import static net.andreinc.mockneat.unit.id.UUIDs.uuids;
import static net.andreinc.mockneat.unit.text.Strings.strings;
import static net.andreinc.mockneat.unit.types.Doubles.doubles;
import static net.andreinc.mockneat.unit.user.Names.names;

import de.ubleipzig.kafka.producer.templates.ActivityStream;

import java.time.LocalDate;
import java.util.Optional;

import net.andreinc.mockneat.MockNeat;
import net.andreinc.mockneat.utils.file.FileManager;

/**
 * RandomUtils.
 */
public class RandomMessage {
    private final FileManager fm = FileManager.getInstance();
    private LocalDate localDate;
    private String summary;
    private String actorId;
    private Double actorType;
    private String actorName;
    private String actorUrl;
    private String objectId;
    private String objectName;
    private String objectUrl;
    private String targetId;
    private String targetType;
    private String targetName;

    public RandomMessage() {
        MockNeat mock = MockNeat.threadLocal();
        this.localDate = mock.localDates().val();
        this.summary = strings().get();
        this.actorId = uuids().get();
        this.actorType = doubles().range(2000.0, 10000.0).get();
        this.actorName = names().last().get() + ", " + names().first().get();
        this.actorUrl = strings().get();
        this.objectId = uuids().get();
        this.objectName = countries().names().get();
        this.objectUrl = strings().get();
        this.targetId = uuids().get();
        this.targetType = strings().get();
        this.targetName = strings().get();
    }
    public String buildRandomActivityStreamMessage() {
        final ActivityStream stream = new ActivityStream();
        stream.setContext("https://www.w3.org/ns/activitystreams");
        stream.setSummary(this.summary);
        stream.setPublished(this.localDate.toString());
        stream.setType("Create");
        final ActivityStream.Actor actor = new ActivityStream.Actor();
        actor.setId(this.actorId);
        actor.setType(this.actorType);
        actor.setName(this.actorName);
        actor.setUrl(this.actorUrl);
        stream.setActor(actor);
        final ActivityStream.Object object = new ActivityStream.Object();
        object.setId(this.objectId);
        object.setType("Article");
        object.setName(this.objectName);
        object.setUrl(this.objectUrl);
        stream.setObject(object);
        final ActivityStream.Target target = new ActivityStream.Target();
        target.setId(this.targetId);
        target.setName(this.targetName);
        target.setType(this.targetType);
        stream.setTarget(target);
        Optional<String> json = serialize(stream);
        return json.orElse(null);
    }
}
