/*
 *
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *   FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *   OR LOSS OR CORRUPTION OF DATA.
 *
 */
package com.hortonworks.hivestudio.eventProcessor.pipeline;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * An unmodifiable view over a map with an adapter function to change value from V1 to V2.
 *
 * @param <K> The key type.
 * @param <V1> The input map value type.
 * @param <V2> The view map value type.
 */
public class ImmutableMapView<K, V1, V2> implements Map<K, V2> {
  private final Map<K, V1> delegate;
  private final Function<V1, V2> mapFn;

  public ImmutableMapView(Map<K, V1> delegate, Function<V1, V2> mapFn) {
    this.delegate = delegate;
    this.mapFn = mapFn;
  }

  private V2 convert(V1 val) {
    return val == null ? null : mapFn.apply(val);
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return delegate.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    for (V1 val : delegate.values()) {
      if (Objects.equals(convert(val), value)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public V2 get(Object key) {
    return convert(delegate.get(key));
  }

  @Override
  public V2 put(K key, V2 value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public V2 remove(Object key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends K, ? extends V2> m) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<K> keySet() {
    return Collections.unmodifiableSet(delegate.keySet());
  }

  @Override
  public Collection<V2> values() {
    final Collection<V1> vals = delegate.values();
    return new AbstractCollection<V2>() {
      @Override
      public Iterator<V2> iterator() {
        Iterator<V1> iter = vals.iterator();
        return new Iterator<V2>() {
          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }
          @Override
          public V2 next() {
            return convert(iter.next());
          }
        };
      }

      @Override
      public int size() {
        return vals.size();
      }
    };
  }

  @Override
  public Set<Entry<K, V2>> entrySet() {
    Set<Entry<K, V1>> entries = delegate.entrySet();
    return new AbstractSet<Entry<K, V2>>() {

      @Override
      public Iterator<Entry<K, V2>> iterator() {
        Iterator<Entry<K, V1>> iter = entries.iterator();
        return new Iterator<Entry<K,V2>>() {

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public Entry<K, V2> next() {
            Entry<K, V1> entry = iter.next();
            return new Entry<K, V2>() {
              @Override
              public K getKey() {
                return entry.getKey();
              }

              @Override
              public V2 getValue() {
                return convert(entry.getValue());
              }

              @Override
              public V2 setValue(V2 value) {
                throw new UnsupportedOperationException();
              }
            };
          }
        };
      }

      @Override
      public int size() {
        return entries.size();
      }
    };
  }
}
