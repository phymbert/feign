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
package feign.optionals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import feign.Feign;
import feign.RequestLine;
import feign.codec.DecodeException;
import feign.jackson.JacksonEncoder;
import feign.jackson.JacksonIterator;
import feign.stream.Java8StreamDecoder;
import feign.stream.Java8StreamEncoder;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamTest {

  interface StreamInterface {
    @RequestLine("GET /")
    Stream<String> get();

    @RequestLine("GET /streamOutput")
    Stream<Car> getCars();

    @RequestLine("GET /streamInputAndOutput")
    Stream<Car> largeData(Stream<Car> input);

    class Car {
      public String name;
      public String manufacturer;
    }
  }

  private String carsJson = ""//
      + "[{\r\n"//
      + "  \"name\" : \"Megane\",\r\n"//
      + "  \"manufacturer\" : \"Renault\"\r\n"//
      + "},{\r\n"//
      + "  \"name\" : \"C4\",\r\n"//
      + "  \"manufacturer\" : \"Citroën\"\r\n"//
      + "}]";

  @Test
  public void simpleStreamTest() throws IOException, InterruptedException {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setBody("foo\nbar"));

    StreamInterface api = Feign.builder()
        .decoder(new Java8StreamDecoder((type, response) -> {
          try {
            return new BufferedReader(new InputStreamReader(response.body().asInputStream())).lines().iterator();
          } catch (IOException e) {
            throw new DecodeException(e.getMessage(), e);
          }
        })).target(StreamInterface.class, server.url("/").toString());

    try (Stream<String> stream = api.get()) {
      assertThat(stream.collect(Collectors.toList())).isEqualTo(Arrays.asList("foo", "bar"));
    }
  }

  @Test
  public void simpleJsonStreamTest() throws IOException, InterruptedException {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setBody(carsJson));

    ObjectMapper mapper = new ObjectMapper();

    StreamInterface api = Feign.builder()
        .decoder(new Java8StreamDecoder((type, response) -> JacksonIterator.<StreamInterface.Car>builder().of(type).mapper(mapper).response(response).build()))
        .target(StreamInterface.class, server.url("/").toString());

    try (Stream<StreamInterface.Car> stream = api.getCars()) {
      assertThat(stream.collect(Collectors.toList())).hasSize(2);
    }
  }

  @Test
  public void streamsLargeTest() throws IOException, InterruptedException {
    MockWebServer server = new MockWebServer();
    server.enqueue(new MockResponse().setBody(carsJson));

    ObjectMapper mapper = new ObjectMapper();
    StreamInterface api = Feign.builder()
        .decoder(new Java8StreamDecoder((type, response) -> JacksonIterator.<StreamInterface.Car>builder().of(type).mapper(mapper).response(response).build()))
        .encoder(new Java8StreamEncoder(new JacksonEncoder(), "[", "]", ","))
        .target(StreamInterface.class, server.url("/").toString());

    try (Stream<StreamInterface.Car> stream = api.largeData(cars(mapper).stream())) {
      assertThat(stream.collect(Collectors.toList())).hasSize(2);
    }
    RecordedRequest recordedRequest = server.takeRequest();
    assertThat(recordedRequest.getBody().readUtf8()).isEqualTo(carsJson);
  }

  private List<StreamInterface.Car> cars(ObjectMapper mapper) throws IOException {
    return mapper.readValue(carsJson,
          new TypeReference<List<StreamInterface.Car>>(){});
  }
}
