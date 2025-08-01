/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.stream;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


public class StreamDataDecoderImplTest {
  private static final String NAME_FIELD = "name";
  private static final String AGE_HEADER_KEY = "age";
  private static final String SEQ_NO_RECORD_METADATA = "seqNo";
  private static final StreamMessageMetadata METADATA = mock(StreamMessageMetadata.class);

  @Test
  public void testDecodeValueOnly() {
    TestDecoder messageDecoder = new TestDecoder();
    messageDecoder.init(Map.of(), Set.of(NAME_FIELD), "");
    String value = "Alice";
    BytesStreamMessage message = new BytesStreamMessage(value.getBytes(StandardCharsets.UTF_8), METADATA);
    StreamDataDecoderResult result = new StreamDataDecoderImpl(messageDecoder).decode(message);
    Assert.assertNotNull(result);
    Assert.assertNull(result.getException());
    Assert.assertNotNull(result.getResult());

    GenericRow row = result.getResult();
    Assert.assertEquals(row.getFieldToValueMap().size(), 2);
    Assert.assertEquals(String.valueOf(row.getValue(NAME_FIELD)), value);
    Assert.assertEquals(row.getValue(StreamDataDecoderImpl.RECORD_SERIALIZED_VALUE_SIZE_KEY), value.length());
  }

  @Test
  public void testDecodeKeyAndHeaders() {
    TestDecoder messageDecoder = new TestDecoder();
    messageDecoder.init(Map.of(), Set.of(NAME_FIELD), "");
    String value = "Alice";
    String key = "id-1";
    GenericRow headers = new GenericRow();
    headers.putValue(AGE_HEADER_KEY, 3);
    StreamMessageMetadata metadata = new StreamMessageMetadata.Builder().setRecordIngestionTimeMs(1234L)
        .setOffset(new LongMsgOffset(0), new LongMsgOffset(1))
        .setHeaders(headers)
        .setMetadata(Map.of(SEQ_NO_RECORD_METADATA, "1"))
        .build();
    BytesStreamMessage message =
        new BytesStreamMessage(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8), metadata);

    StreamDataDecoderResult result = new StreamDataDecoderImpl(messageDecoder).decode(message);
    Assert.assertNotNull(result);
    Assert.assertNull(result.getException());
    Assert.assertNotNull(result.getResult());

    GenericRow row = result.getResult();
    Assert.assertEquals(row.getFieldToValueMap().size(), 5);
    Assert.assertEquals(row.getValue(NAME_FIELD), value);
    Assert.assertEquals(row.getValue(StreamDataDecoderImpl.KEY), key, "Failed to decode record key");
    Assert.assertEquals(row.getValue(StreamDataDecoderImpl.HEADER_KEY_PREFIX + AGE_HEADER_KEY), 3);
    Assert.assertEquals(row.getValue(StreamDataDecoderImpl.METADATA_KEY_PREFIX + SEQ_NO_RECORD_METADATA), "1");
    Assert.assertEquals(row.getValue(StreamDataDecoderImpl.RECORD_SERIALIZED_VALUE_SIZE_KEY), value.length());
  }

  @Test
  public void testNoExceptionIsThrown() {
    ThrowingDecoder messageDecoder = new ThrowingDecoder();
    messageDecoder.init(Map.of(), Set.of(NAME_FIELD), "");
    String value = "Alice";
    BytesStreamMessage message = new BytesStreamMessage(value.getBytes(StandardCharsets.UTF_8), METADATA);
    StreamDataDecoderResult result = new StreamDataDecoderImpl(messageDecoder).decode(message);
    Assert.assertNotNull(result);
    Assert.assertNotNull(result.getException());
    Assert.assertNull(result.getResult());
  }

  private static class ThrowingDecoder implements StreamMessageDecoder<byte[]> {

    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName) {
    }

    @Nullable
    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      throw new RuntimeException("something failed during decoding");
    }

    @Nullable
    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      throw new RuntimeException("something failed during decoding");
    }
  }

  private static class TestDecoder implements StreamMessageDecoder<byte[]> {
    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName) {
    }

    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      destination.putValue(NAME_FIELD, new String(payload, StandardCharsets.UTF_8));
      return destination;
    }

    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      return decode(payload, destination);
    }
  }
}
