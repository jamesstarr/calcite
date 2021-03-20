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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.janino.JaninoMetadataHandlerCreator;
import org.apache.calcite.util.ControlFlowException;
import org.apache.calcite.util.Util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.UncheckedExecutionException;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;

/**
 * Implementation of the {@link RelMetadataProvider} interface that generates
 * a class that dispatches to the underlying providers.
 */
public class JaninoRelMetadataProvider implements RelMetadataProvider {
  private final RelMetadataProvider provider;

  // Constants and static fields

  public static final JaninoRelMetadataProvider DEFAULT =
      JaninoRelMetadataProvider.of(DefaultRelMetadataProvider.INSTANCE);


  /** Cache of pre-generated handlers by provider and kind of metadata.
   * For the cache to be effective, providers should implement identity
   * correctly. */
  @SuppressWarnings("unchecked")
  private static final LoadingCache<Key, MetadataHandler> HANDLERS =
      maxSize(CacheBuilder.newBuilder(),
          CalciteSystemProperty.METADATA_HANDLER_CACHE_MAXIMUM_SIZE.value())
          .build(
              CacheLoader.from(key ->
                  load3(key.def, key.provider.handlers(key.def))));

  // Pre-register the most common relational operators, to reduce the number of
  // times we re-generate.

  /** Private constructor; use {@link #of}. */
  private JaninoRelMetadataProvider(RelMetadataProvider provider) {
    this.provider = provider;
  }

  /** Creates a JaninoRelMetadataProvider.
   *
   * @param provider Underlying provider
   */
  public static JaninoRelMetadataProvider of(RelMetadataProvider provider) {
    if (provider instanceof JaninoRelMetadataProvider) {
      return (JaninoRelMetadataProvider) provider;
    }
    return new JaninoRelMetadataProvider(provider);
  }

  // helper for initialization
  private static <K, V> CacheBuilder<K, V> maxSize(CacheBuilder<K, V> builder,
      int size) {
    if (size >= 0) {
      builder.maximumSize(size);
    }
    return builder;
  }

  @Override public boolean equals(@Nullable Object obj) {
    return obj == this
        || obj instanceof JaninoRelMetadataProvider
        && ((JaninoRelMetadataProvider) obj).provider.equals(provider);
  }

  @Override public int hashCode() {
    return 109 + provider.hashCode();
  }

  @Deprecated // to be removed before 2.0
  @Override public <@Nullable M extends @Nullable Metadata> UnboundMetadata<M> apply(
      Class<? extends RelNode> relClass, Class<? extends M> metadataClass) {
    throw new UnsupportedOperationException();
  }

  @Override public <M extends Metadata> Multimap<Method, MetadataHandler<M>>
      handlers(MetadataDef<M> def) {
    return provider.handlers(def);
  }

  private static <M extends Metadata> MetadataHandler<M> load3(
      MetadataDef<M> def, Multimap<Method, ? extends MetadataHandler<?>> map) {
    return JaninoMetadataHandlerCreator.newInstance(def.handlerClass,
        map);
  }

  synchronized <M extends Metadata, H extends MetadataHandler<M>> H create(
      MetadataDef<M> def) {
    try {
      final Key key = new Key(def, provider);
      //noinspection unchecked
      return (H) HANDLERS.get(key);
    } catch (UncheckedExecutionException | ExecutionException e) {
      throw Util.throwAsRuntime(Util.causeOrSelf(e));
    }
  }

  synchronized <M extends Metadata, H extends MetadataHandler<M>> H revise(
      Class<? extends RelNode> rClass, MetadataDef<M> def) {
    //noinspection unchecked
    return (H) create(def);
  }

  /** Registers some classes. Does not flush the providers, but next time we
   * need to generate a provider, it will handle all of these classes. So,
   * calling this method reduces the number of times we need to re-generate. */
  @Deprecated
  public void register(Iterable<Class<? extends RelNode>> classes) {
  }

  /** Exception that indicates there there should be a handler for
   * this class but there is not. The action is probably to
   * re-generate the handler class. */
  public static class NoHandler extends ControlFlowException {
    public final Class<? extends RelNode> relClass;

    public NoHandler(Class<? extends RelNode> relClass) {
      this.relClass = relClass;
    }
  }

  /** Key for the cache. */
  private static class Key {
    public final MetadataDef def;
    public final RelMetadataProvider provider;

    private Key(MetadataDef def, RelMetadataProvider provider) {
      this.def = def;
      this.provider = provider;
    }

    @Override public int hashCode() {
      return (def.hashCode() * 37
          + provider.hashCode()) * 37;
    }

    @Override public boolean equals(@Nullable Object obj) {
      return this == obj
          || obj instanceof Key
          && ((Key) obj).def.equals(def)
          && ((Key) obj).provider.equals(provider);
    }
  }
}
