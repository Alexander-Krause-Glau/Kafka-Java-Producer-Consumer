package org.krause.kafka.binary.util;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.krause.kafka.Message;

public class MessageToByteSerializer implements Serializer<Message> {

  private static final int BYTE_BUFFER_CAPACITY = 100;
  private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

  private final ByteBufferSerializer byteBufferSerializer = new ByteBufferSerializer();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.byteBufferSerializer.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, Message data) {
    final ByteBuffer buffer = ByteBuffer.allocateDirect(BYTE_BUFFER_CAPACITY);

    putStringIntoByteArray(data.getId(), buffer);
    putStringIntoByteArray(data.getTimestamp(), buffer);
    putStringIntoByteArray(data.getData(), buffer);

    buffer.flip();
    return this.byteBufferSerializer.serialize(topic, buffer);
  }

  @Override
  public void close() {
    this.byteBufferSerializer.close();
  }

  private void putStringIntoByteArray(String value, ByteBuffer buffer) {
    final byte[] stringBytes = value.getBytes(DEFAULT_CHARSET);
    buffer.putInt(stringBytes.length);
    buffer.put(stringBytes);
  }

}
