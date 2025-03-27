package org.apache.hadoop.ozone.om.lock.granular;

import jakarta.annotation.Nonnull;
import org.apache.ratis.util.Preconditions;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/** Locks for components volume, bucket and keys. */
public class OmComponentLock implements Comparable<OmComponentLock> {

  public enum Component {
    VOLUME, BUCKET, KEY
  }

  public enum Type {
    READ, WRITE
  }

  private final String name;
  private final Component component;
  private final Type type;
  private final Lock lock;
  private boolean locked = false;

  OmComponentLock(String name, Component component, Type type, ReadWriteLock lock) {
    Objects.requireNonNull(lock, "lock == null");
    this.name = Objects.requireNonNull(name, "name == null");
    this.component = Objects.requireNonNull(component, "component == null");
    this.type = Objects.requireNonNull(type, "type == null");
    this.lock = type == Type.READ ? lock.readLock() : lock.writeLock();
  }

  void acquire() {
    Preconditions.assertTrue(!locked, () -> this + " is already acquired");
    lock.lock();
    locked = true;
  }

  void release() {
    Preconditions.assertTrue(locked, () -> this + " is NOT yet acquired");
    locked = false;
    lock.unlock();
  }

  @Override
  public int compareTo(@Nonnull OmComponentLock that) {
    final int diff = this.component.compareTo(that.component);
    return diff != 0 ? diff : this.name.compareTo(that.name);
  }

  @Override
  public String toString() {
    return name + ":" + component + "_" + type + ":" + (locked ? "LOCKED" : "unlocked");
  }
}
