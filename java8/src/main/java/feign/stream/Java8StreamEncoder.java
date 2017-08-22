/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feign.stream;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import feign.RequestTemplate;
import feign.codec.EncodeException;
import feign.codec.Encoder;

import static feign.Util.CONTENT_LENGTH;

/**
 * Wrap a base encoder and stream body encoding to input stream.
 * For example: <br>
 * <pre>
 * Feign.builder()
 * .encoder(new Java8StreamEncoder(new JacksonEncoder()))
 * .target(GitHub.class, "https://api.github.com");
 * </pre>
 */
public class Java8StreamEncoder implements Encoder {

  private final Encoder chunkEncoder;
  private final byte[] startDelimiter;
  private final byte[] endDelimiter;
  private final byte[] delimiter;

  public Java8StreamEncoder() {
    this(new Encoder.Default());
  }

  public Java8StreamEncoder(byte[] delimiter) {
    this(new Encoder.Default(), null, null, delimiter);
  }

  public Java8StreamEncoder(final byte[] startDelimiter, final byte[] endDelimiter, byte[] delimiter) {
    this(new Encoder.Default(), startDelimiter, endDelimiter, delimiter);
  }

  public Java8StreamEncoder(final Encoder chunkEncoder) {
    this.chunkEncoder = chunkEncoder;
    this.startDelimiter = null;
    this.endDelimiter = null;
    this.delimiter = null;
  }

  public Java8StreamEncoder(final Encoder chunkEncoder, final String startDelimiter, final String endDelimiter, final String delimiter) {
    this(chunkEncoder, startDelimiter.getBytes(), endDelimiter.getBytes(), delimiter.getBytes());
  }

  public Java8StreamEncoder(final Encoder chunkEncoder, final byte[] startDelimiter, final byte[] endDelimiter, final byte[] delimiter) {
    this.chunkEncoder = chunkEncoder;
    this.startDelimiter = startDelimiter;
    this.endDelimiter = endDelimiter;
    this.delimiter = delimiter;
  }

  @Override
  public void encode(Object object, Type bodyType, RequestTemplate template)
      throws EncodeException {
    if (!(object instanceof Stream)) {
      throw new IllegalArgumentException(
          "Java8StreamEncoder supports only stream: unknown parameter " + object);
    }
    final Stream<?> stream = (Stream<?>) object;

    if (!(bodyType instanceof ParameterizedType)) {
      throw new IllegalArgumentException(
          "Java8StreamEncoder supports only stream: unknown " + bodyType);
    }
    final ParameterizedType parameterizedType = (ParameterizedType) bodyType;
    if (!Stream.class.equals(parameterizedType.getRawType())) {
      throw new IllegalArgumentException(
          "Java8StreamEncoder supports only stream: unknown " + bodyType);
    }
    final Class<?> streamedType = (Class<?>) parameterizedType.getActualTypeArguments()[0];

    Integer contentLength = provideContentLength();
    template.streamBody(provideInputStream(stream, streamedType, template), contentLength);
    if (contentLength == null) {
      template.header(CONTENT_LENGTH);
    }
  }

  protected InputStream provideInputStream(final Stream<?> stream, final Class<?> streamedType,
                                         final RequestTemplate template) {
    final AtomicBoolean first = new AtomicBoolean(true);
    final Stream<InputStream> inputStreamStreamBase = stream.map(o -> {
      chunkEncoder.encode(o, streamedType, template);
      byte[] body = template.body();
      template.body(null, null);
      if (delimiter != null) {
        if (!first.getAndSet(false)) {
          body = ByteBuffer.allocate(body.length + delimiter.length)
              .put(delimiter)
              .put(body)
              .array();
        }
      }
      return body;
    }).map(ByteArrayInputStream::new);

    Stream<InputStream> inputStreamStream = inputStreamStreamBase;
    if (startDelimiter != null) {
      inputStreamStream = Stream.concat(Stream.of(new ByteArrayInputStream(startDelimiter)), inputStreamStream);
    }
    if (endDelimiter != null) {
      inputStreamStream = Stream.concat(inputStreamStream, Stream.of(new ByteArrayInputStream(endDelimiter)));
    }

    return new SequenceInputStream(new InputStreamEnumeration(inputStreamStream.iterator()));
  }

  protected Integer provideContentLength() {
    return null;
  }

  private class InputStreamEnumeration implements Enumeration<InputStream> {

    private final Iterator<? extends InputStream> iterator;

    public InputStreamEnumeration(final Iterator<? extends InputStream> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasMoreElements() {
      return this.iterator.hasNext();
    }

    @Override
    public InputStream nextElement() {
      return this.iterator.next();
    }
  }
}
