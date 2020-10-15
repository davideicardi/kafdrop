package kafdrop.service;

import java.util.Properties;

import com.davideicardi.kaa.KaaSchemaRegistry;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.stereotype.Service;

import kafdrop.config.KafkaConfiguration;

@Service
public final class KaaService {
  private KaaSchemaRegistry schemaRegistry;

  public KaaSchemaRegistry getSchemaRegistry() { return schemaRegistry; }

  public KaaService(KafkaConfiguration kafkaConfiguration) {
    final var producerProps = new Properties();
    kafkaConfiguration.applyCommon(producerProps);
    producerProps.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, "kafdrop");

    final var consumerProps = new Properties();
    kafkaConfiguration.applyCommon(consumerProps);
    consumerProps.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, "kafdrop");

    this.schemaRegistry = new KaaSchemaRegistry(
        producerProps,
        consumerProps
      );
  }

  public void close() {
    this.schemaRegistry.close();
  }
}