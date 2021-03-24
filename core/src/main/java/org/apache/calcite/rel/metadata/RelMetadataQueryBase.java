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

import org.apache.calcite.rel.RelNode;

import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Supplier;

/**
 * Base class for the RelMetadataQuery that uses the metadata handler class
 * generated by the Janino.
 *
 * <p>To add a new implementation to this interface, follow
 * these steps:
 *
 * <ol>
 * <li>Extends {@link RelMetadataQuery} (name it MyRelMetadataQuery for example)
 * to reuse the Calcite builtin metadata query interfaces. In this class, define all the
 * extended Handlers for your metadata and implement the metadata query interfaces.
 * <li>Write your customized provider class <code>RelMdXyz</code>. Follow
 * the pattern from an existing class such as {@link RelMdColumnOrigins},
 * overloading on all of the logical relational expressions to which the query
 * applies.
 * <li>Add a {@code SOURCE} static member to each of your provider class, similar to
 * {@link RelMdColumnOrigins#SOURCE}.
 * <li>Extends {@link DefaultRelMetadataProvider} (name it MyRelMetadataProvider for example)
 * and supplement the "SOURCE"s into the builtin list
 * (This is not required, use {@link ChainedRelMetadataProvider} to chain your customized
 * "SOURCE"s with default ones also works).
 * <li>Set {@code MyRelMetadataProvider} into the cluster instance.
 * <li>Use
 * {@link org.apache.calcite.plan.RelOptCluster#setMetadataQuerySupplier(Supplier)}
 * to set the metadata query {@link Supplier} into the cluster instance. This {@link Supplier}
 * should return a <strong>fresh new</strong> instance.
 * <li>Use the cluster instance to create
 * {@link org.apache.calcite.sql2rel.SqlToRelConverter}.</li>
 * <li>Query your metadata within {@link org.apache.calcite.plan.RelOptRuleCall} with the
 * interfaces you defined in {@code MyRelMetadataQuery}.
 * </ol>
 */
public class RelMetadataQueryBase {

  //~ Instance fields --------------------------------------------------------

  /** Set of active metadata queries, and cache of previous results. */
  @Deprecated // to be removed before 2.0
  public final Table<RelNode, Object, Object> map;

  public final MetadataCache cache;
  protected final MetadataHandlerProvider metadataHandlerProvider;

  @Deprecated // to be removed before 2.0
  public final @Nullable JaninoRelMetadataProvider metadataProvider = THREAD_PROVIDERS.get();

  //~ Static fields/initializers ---------------------------------------------

  @Deprecated // to be removed before 2.0
  public static final ThreadLocal<@Nullable JaninoRelMetadataProvider> THREAD_PROVIDERS =
      new ThreadLocal<>();

  @Deprecated // to be removed before 2.0
  protected static <H> H initialHandler(Class<H> handlerClass) {
    return LegacyJaninoMetadataHandlerProvider.INSTANCE.initialHandler(handlerClass);
  }

  //~ Constructors ----------------------------------------------------------

  protected RelMetadataQueryBase(MetadataHandlerProvider metadataHandlerProvider) {
    this.metadataHandlerProvider = metadataHandlerProvider;
    this.cache = metadataHandlerProvider.buildCache();
    if (cache instanceof TableMetadataCache) {
      map = ((TableMetadataCache) cache).map;
    } else {
      map = ImmutableTable.of();
    }
  }

  protected RelMetadataQueryBase(MetadataHandlerProvider metadataHandlerProvider, boolean isProto) {
    this.metadataHandlerProvider = metadataHandlerProvider;
    if (isProto) {
      cache = ErrorCache.INSTANCE;
      map = ImmutableTable.of();
    } else {
      this.cache = metadataHandlerProvider.buildCache();
      if (cache instanceof TableMetadataCache) {
        map = ((TableMetadataCache) cache).map;
      } else {
        map = ImmutableTable.of();
      }
    }
  }

  //~ Methods ----------------------------------------------------------------

  /** Re-generates the handler for a given kind of metadata, adding support for
   * {@code class_} if it is not already present. */
  @Deprecated // to be removed before 2.0
  protected <M extends Metadata, H extends MetadataHandler<M>> H
      revise(Class<? extends RelNode> class_, MetadataDef<M> def) {
    return metadataHandlerProvider.revise((Class<H>) def.handlerClass);
  }

  /**
   * Removes cached metadata values for specified RelNode.
   *
   * @param rel RelNode whose cached metadata should be removed
   * @return true if cache for the provided RelNode was not empty
   */
  @Deprecated // to be removed before 2.0
  public boolean clearCache(RelNode rel) {
    return cache.clear(rel);
  }

  /**
   * Used in prototypes to fail fast.
   */
  private static class ErrorCache implements MetadataCache {
    private static final ErrorCache INSTANCE = new ErrorCache();

    @Override public boolean clear(RelNode rel) {
      throw new UnsupportedOperationException("Prototype query");
    }

    @Override public @Nullable Object remove(RelNode relNode, Object args) {
      throw new UnsupportedOperationException("Prototype query");
    }

    @Override public @Nullable Object get(RelNode relNode, Object args) {
      throw new UnsupportedOperationException("Prototype query");
    }

    @Override public @Nullable Object put(RelNode relNode, Object args, Object value) {
      throw new UnsupportedOperationException("Prototype query");
    }
  }
}
