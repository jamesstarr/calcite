/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
    /**
     * Was {@link #closer} consumer already called ?
     */
    private boolean closed;

    /**
     * Keeps last value of {@code scrollId} in memory so scroll can be released upon termination
     */
    private String scrollId;

    private AutoClosingIterator(
        final Iterator<ElasticsearchJson.Result> delegate,
        final Consumer<String> closer) {
      this.delegate = delegate;
      this.closer = closer;
    }

    @Override public void close() {
      if (!closed && scrollId != null) {
        // close once (if scrollId is present)
        closer.accept(scrollId);
      }
      closed = true;
    }

    @Override public boolean hasNext() {
      final boolean hasNext = delegate.hasNext();
      if (!hasNext) {
        close();
      }
      return hasNext;
    }

    @Override public ElasticsearchJson.Result next() {
      ElasticsearchJson.Result next = delegate.next();
      next.scrollId().ifPresent(id -> scrollId = id);
      return next;
    }
  }

  /**
   * Iterator which consumes current {@code scrollId} until full search result is fetched
   * or {@code limit} is reached.
   */
  private static class SequentialIterator
      extends AbstractSequentialIterator<ElasticsearchJson.Result> {

    private final ElasticsearchTransport transport;
    private final long limit;
    private long count;

    private SequentialIterator(final ElasticsearchJson.Result first,
        final ElasticsearchTransport transport, final long limit) {
      super(first);
      this.transport = transport;
      Preconditions.checkArgument(limit >= 0,
          "limit: %s >= 0", limit);
      this.limit = limit;
    }

    @Override protected ElasticsearchJson.Result computeNext(
        final ElasticsearchJson.Result previous) {
      final int hits = previous.searchHits().hits().size();
      if (hits == 0 || count >= limit) {
        // stop (re-)requesting when limit is reached or no more results
        return null;
      }

      count += hits;
      final String scrollId = previous.scrollId()
          .orElseThrow(() -> new IllegalStateException("scrollId has to be present"));

      return transport.scroll().apply(scrollId);
    }
  }
}
<<<<<<< HEAD
=======

// End Scrolling.java
>>>>>>> 1e9b7360a ([CALCITE-2651] Enable scrolling in ElasticSearch adapter for basic search queries)
