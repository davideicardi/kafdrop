package kafdrop.util;

import java.nio.*;

import com.davideicardi.kaa.KaaSchemaRegistry;
import com.davideicardi.kaa.avro.AvroSingleObjectEncoding;
import com.davideicardi.kaa.avro.GenericAvroSingleObjectSerializer;

public final class KaaAvroMessageDeserializer implements MessageDeserializer {
  private KaaSchemaRegistry schemaRegistry;

  public KaaAvroMessageDeserializer(KaaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @Override
  public String deserializeMessage(ByteBuffer buffer) {
    // Convert byte buffer to byte array
    final var bytes = ByteUtils.convertToByteArray(buffer);
    final var serde = new GenericAvroSingleObjectSerializer(this.schemaRegistry);
    return serde.deserialize(bytes).toString();
  }
}
