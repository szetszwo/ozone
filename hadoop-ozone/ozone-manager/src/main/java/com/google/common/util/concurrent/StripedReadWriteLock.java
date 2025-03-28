package com.google.common.util.concurrent;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StripedReadWriteLock {
  public static class LockAndIndex implements Comparable<LockAndIndex> {
    private final ReentrantReadWriteLock lock;
    private final int index;

    LockAndIndex(ReentrantReadWriteLock lock, int index) {
      this.lock = lock;
      this.index = index;
    }

    public ReentrantReadWriteLock getLock() {
      return lock;
    }

    public int getIndex() {
      return index;
    }

    @Override
    public int compareTo(@Nonnull LockAndIndex that) {
      return Integer.compare(this.index, that.index);
    }
  }

  public static StripedReadWriteLock newInstance(int stripes, boolean fair) {
    final Striped<ReentrantReadWriteLock> striped = Striped.custom(
        stripes, () -> new ReentrantReadWriteLock(fair));
    return new StripedReadWriteLock(striped);
  }

  private final Striped<ReentrantReadWriteLock> striped;

  private StripedReadWriteLock(Striped<ReentrantReadWriteLock> striped) {
    this.striped = striped;
  }

  public LockAndIndex get(Object obj) {
    final int index = striped.indexFor(obj);
    final ReentrantReadWriteLock lock = striped.getAt(index);
    return new LockAndIndex(lock, index);
  }

  public Iterable<LockAndIndex> bulkGet(Iterable<?> objects) {
    final Map<Integer, ReentrantReadWriteLock> sorted = new TreeMap<>();
    final List<LockAndIndex> list = new ArrayList<>();
    for (Object obj : objects) {
      final int index = striped.indexFor(obj);
      final ReentrantReadWriteLock lock = sorted.computeIfAbsent(index, striped::getAt);
      list.add(new LockAndIndex(lock, index));
    }
    list.sort(Comparator.naturalOrder());
    return Collections.unmodifiableList(list);
  }
}
