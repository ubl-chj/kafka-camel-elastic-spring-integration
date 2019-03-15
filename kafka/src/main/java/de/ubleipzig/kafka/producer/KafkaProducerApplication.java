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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.TopicPartitionInitialOffset;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.MessageBuilder;

/**
 * KafkaProducerApplication.
 * @author Christopher Johnson
 */
@SpringBootApplication
@EnableConfigurationProperties(KafkaProducerAppProperties.class)
public class KafkaProducerApplication {

    private KafkaProducerAppProperties producerProperties;
    private IntegrationFlowContext flowContext;
    private KafkaProperties kafkaProperties;

    @Autowired
    public KafkaProducerApplication(KafkaProducerAppProperties producerProperties, KafkaProperties kafkaProperties,
                                    IntegrationFlowContext flowContext) {
        this.producerProperties = producerProperties;
        this.flowContext = flowContext;
        this.kafkaProperties = kafkaProperties;
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext context = new SpringApplicationBuilder(KafkaProducerApplication.class).web(
                WebApplicationType.NONE).run(args);
        context.getBean(KafkaProducerApplication.class).run(context);
        context.close();
    }

    private void run(ConfigurableApplicationContext context) {
        MessageChannel toKafka = context.getBean("toKafka", MessageChannel.class);
        PollableChannel fromKafka = context.getBean("fromKafka", PollableChannel.class);
        System.out.println("Adding an adapter for a topic and sending 10 messages...");
        addListenerForTopics(this.producerProperties.getTopic());

        for (int i = 0; i < 100; i++) {
            final String randomJson = new RandomMessage().buildRandomActivityStreamMessage();
            Message<?> message = MessageBuilder.withPayload(randomJson).setHeader(
                    KafkaHeaders.TOPIC, this.producerProperties.getTopic()).build();
            toKafka.send(message);
        }

        Message<?> received = fromKafka.receive(10000);
        int count = 0;
        while (received != null) {
            System.out.println(received);
            received = fromKafka.receive(++count < 10 ? 10000 : 1000);
        }
    }

    @Bean
    public ProducerFactory<Integer, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.producerProperties.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    @Bean
    public ProducerFactory<Integer, String> kafkaProducerFactory(KafkaProperties properties) {
        Map<String, Object> producerProperties = properties.buildProducerProperties();
        producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        return new DefaultKafkaProducerFactory<>(producerProperties);
    }

    @Bean
    public KafkaTemplate<Integer, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @ServiceActivator(inputChannel = "toKafka")
    @Bean
    public MessageHandler handler(KafkaTemplate<Integer, String> kafkaTemplate) {
        KafkaProducerMessageHandler<Integer, String> handler = new KafkaProducerMessageHandler<>(kafkaTemplate);
        handler.setMessageKeyExpression(new LiteralExpression(this.producerProperties.getMessageKey()));
        return handler;
    }

    @Bean
    public ConsumerFactory<?, ?> kafkaConsumerFactory(KafkaProperties properties) {
        Map<String, Object> consumerProperties = properties.buildConsumerProperties();
        consumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        return new DefaultKafkaConsumerFactory<>(consumerProperties);
    }

    @Bean
    public KafkaMessageListenerContainer<String, String> container(
            ConsumerFactory<String, String> kafkaConsumerFactory) {
        return new KafkaMessageListenerContainer<>(kafkaConsumerFactory,
                new ContainerProperties(new TopicPartitionInitialOffset(this.producerProperties.getTopic(), 0)));
    }

    /*
     * Boot's autoconfigured KafkaAdmin will provision the topics.
     */

    @Bean
    public KafkaMessageDrivenChannelAdapter<String, String> adapter(
            KafkaMessageListenerContainer<String, String> container) {
        KafkaMessageDrivenChannelAdapter<String, String> kafkaMessageDrivenChannelAdapter =
                new KafkaMessageDrivenChannelAdapter<>(
                container);
        kafkaMessageDrivenChannelAdapter.setOutputChannel(fromKafka());
        return kafkaMessageDrivenChannelAdapter;
    }

    @Bean
    public PollableChannel fromKafka() {
        return new QueueChannel();
    }

    @Bean
    public NewTopic topic(KafkaProducerAppProperties properties) {
        return new NewTopic(properties.getTopic(), 1, (short) 1);
    }

    private void addListenerForTopics(String... topics) {
        Map<String, Object> consumerProperties = kafkaProperties.buildConsumerProperties();
        // change the group id so we don't revoke the other partitions.
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG,
                consumerProperties.get(ConsumerConfig.GROUP_ID_CONFIG) + "x");
        IntegrationFlow flow = IntegrationFlows.from(
                Kafka.messageDrivenChannelAdapter(new DefaultKafkaConsumerFactory<String, String>(consumerProperties),
                        topics)).channel("fromKafka").get();
        this.flowContext.registration(flow).register();
    }
}
