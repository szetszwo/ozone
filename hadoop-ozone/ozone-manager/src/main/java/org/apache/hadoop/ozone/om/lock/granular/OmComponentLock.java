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
  private final int stripeIndex;
  private final Lock stripeLock;
  private boolean locked = false;

  OmComponentLock(String name, Component component, Type type, int stripeIndex, ReadWriteLock stripeLock) {
    Objects.requireNonNull(stripeLock, "stripeLock == null");
    this.name = Objects.requireNonNull(name, "name == null");
    this.component = Objects.requireNonNull(component, "component == null");
    this.type = Objects.requireNonNull(type, "type == null");
    this.stripeIndex = stripeIndex;
    this.stripeLock = type == Type.READ ? stripeLock.readLock() : stripeLock.writeLock();
  }

  void acquire() {
    Preconditions.assertTrue(!locked, () -> this + " is already acquired");
    stripeLock.lock();
    locked = true;
  }

  void release() {
    Preconditions.assertTrue(locked, () -> this + " is NOT yet acquired");
    locked = false;
    stripeLock.unlock();
  }

  @Override
  public int compareTo(@Nonnull OmComponentLock that) {
    final int diff = this.component.compareTo(that.component);
    return diff != 0 ? diff : Integer.compare(this.stripeIndex, that.stripeIndex);
  }

  @Override
  public String toString() {
    return name + ":" + component + "_" + type + ":s" + stripeIndex + "-" + (locked ? "LOCKED" : "unlocked");
  }
}
