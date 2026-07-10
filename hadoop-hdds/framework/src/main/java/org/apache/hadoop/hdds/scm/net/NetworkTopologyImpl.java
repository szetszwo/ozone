/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.net;

import static org.apache.hadoop.hdds.scm.net.NetConstants.ANCESTOR_GENERATION_DEFAULT;
import static org.apache.hadoop.hdds.scm.net.NetConstants.ROOT;
import static org.apache.hadoop.hdds.scm.net.NetConstants.SCOPE_REVERSE_STR;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class represents a cluster of computers with a tree hierarchical
 * network topology. In the network topology, leaves represent data nodes
 * (computers) and inner nodes represent datacenter/core-switches/routers that
 * manages traffic in/out of data centers or racks.
 */
public class NetworkTopologyImpl implements NetworkTopology {
  private static final Logger LOG =
      LoggerFactory.getLogger(NetworkTopologyImpl.class);
  private static final Consumer<List<?>> SHUFFLE = list -> Collections.shuffle(list, ThreadLocalRandom.current());

  /** The root cluster tree. */
  private final InnerNode clusterTree;
  /** Depth of all leaf nodes. */
  private final int maxLevel;
  /** The algorithm to randomize nodes with equal distances. */
  private final Consumer<List<?>> shuffleOperation;
  /** Lock to coordinate cluster tree access. */
  private final ReadWriteLock netlock = new ReentrantReadWriteLock(true);

  private NetworkTopologyImpl(NodeSchemaManager manager, Consumer<List<?>> shuffleOperation, InnerNode clusterTree) {
    this.clusterTree = clusterTree;
    this.maxLevel = manager.getMaxLevel();
    this.shuffleOperation = shuffleOperation;
  }

  public NetworkTopologyImpl(ConfigurationSource conf) {
    this(NodeSchemaManager.getInstance().init(conf));
  }

  public NetworkTopologyImpl(String schemaFile, InnerNode clusterTree) {
    this(NodeSchemaManager.getInstance().init(schemaFile), SHUFFLE, clusterTree);
  }

  @VisibleForTesting
  NetworkTopologyImpl(NodeSchemaManager manager, Consumer<List<?>> shuffleOperation) {
    this(manager, shuffleOperation,
        InnerNodeImpl.FACTORY.newInnerNode(ROOT, null, null,
        NetConstants.ROOT_LEVEL, manager.getCost(NetConstants.ROOT_LEVEL))
    );
  }

  @VisibleForTesting
  public NetworkTopologyImpl(NodeSchemaManager manager) {
    this(manager, SHUFFLE);
  }

  /**
   * Add a leaf node. This will be called when a new datanode is added.
   * @param node node to be added; can be null
   * @exception IllegalArgumentException if add a node to a leave or node to be
   * added is not a leaf
   */
  @Override
  public void add(Node node) {
    Objects.requireNonNull(node, "node == null");
    if (node instanceof InnerNode) {
      throw new IllegalArgumentException(
          "Not allowed to add an inner node: " + node.getNetworkFullPath());
    }
    int newDepth = NetUtils.locationToDepth(node.getNetworkLocation()) + 1;

    // Check depth
    if (maxLevel != newDepth) {
      throw new InvalidTopologyException("Failed to add " +
          node.getNetworkFullPath() + ": Its path depth is not " + maxLevel);
    }
    netlock.writeLock().lock();
    boolean add;
    try {
      add = clusterTree.add(node);
    } finally {
      netlock.writeLock().unlock();
    }

    if (add) {
      LOG.info("Added a new node: {}", node.getNetworkFullPath());
      if (LOG.isDebugEnabled()) {
        LOG.debug("NetworkTopology became:\n{}", this);
      }
    }
  }

  /**
   * Update a leaf node. It is called when a datanode needs to be updated.
   * If the old datanode does not exist, then just add the new datanode.
   * @param oldNode node to be updated; can be null
   * @param newNode node to update to; cannot be null
   */
  @Override
  public void update(Node oldNode, Node newNode) {
    Objects.requireNonNull(newNode, "newNode == null");
    if (oldNode instanceof InnerNode) {
      throw new IllegalArgumentException(
              "Not allowed to update an inner node: "
                      + oldNode.getNetworkFullPath());
    }

    if (newNode instanceof InnerNode) {
      throw new IllegalArgumentException(
              "Not allowed to update a leaf node to an inner node: "
                      + newNode.getNetworkFullPath());
    }

    int newDepth = NetUtils.locationToDepth(newNode.getNetworkLocation()) + 1;
    // Check depth
    if (maxLevel != newDepth) {
      throw new InvalidTopologyException("Failed to update to " +
              newNode.getNetworkFullPath()
              + ": Its path depth is not "
              + maxLevel);
    }

    netlock.writeLock().lock();
    boolean add;
    try {
      boolean exist = false;
      if (oldNode != null) {
        exist = containsNode(oldNode);
      }
      if (exist) {
        clusterTree.remove(oldNode);
      }

      add = clusterTree.add(newNode);
    } finally {
      netlock.writeLock().unlock();
    }
    if (add) {
      LOG.info("Updated to the new node: {}", newNode.getNetworkFullPath());
      if (LOG.isDebugEnabled()) {
        LOG.debug("NetworkTopology became:\n{}", this);
      }
    }
  }

  /**
   * Remove a node from the network topology. This will be called when a
   * existing datanode is removed from the system.
   * @param node node to be removed; cannot be null
   */
  @Override
  public void remove(Node node) {
    Objects.requireNonNull(node, "node == null");
    if (node instanceof InnerNode) {
      throw new IllegalArgumentException(
          "Not allowed to remove an inner node: " + node.getNetworkFullPath());
    }
    netlock.writeLock().lock();
    try {
      clusterTree.remove(node);
    } finally {
      netlock.writeLock().unlock();
    }
    LOG.info("Removed a node: {}", node.getNetworkFullPath());
    if (LOG.isDebugEnabled()) {
      LOG.debug("NetworkTopology became:\n{}", this);
    }
  }

  /**
   * Check if the tree already contains node <i>node</i>.
   * @param node a node
   * @return true if <i>node</i> is already in the tree; false otherwise
   */
  @Override
  public boolean contains(Node node) {
    Objects.requireNonNull(node, "node == null");
    netlock.readLock().lock();
    try {
      return containsNode(node);
    } finally {
      netlock.readLock().unlock();
    }
  }

  private boolean containsNode(Node node) {
    Node parent = node.getParent();
    while (parent != null && !Objects.equals(parent, clusterTree)) {
      parent = parent.getParent();
    }
    return Objects.equals(parent, clusterTree);
  }

  /**
   * Compare the specified ancestor generation of each node for equality.
   * @return true if their specified generation ancestor are equal
   */
  @Override
  public boolean isSameAncestor(Node node1, Node node2, int ancestorGen) {
    if (node1 == null || node2 == null || ancestorGen <= 0) {
      return false;
    }
    netlock.readLock().lock();
    try {
      Node ancestor1 = node1.getAncestor(ancestorGen);
      Node ancestor2 = node2.getAncestor(ancestorGen);
      return Objects.equals(ancestor1, ancestor2);
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Compare the direct parent of each node for equality.
   * @return true if their parent are the same
   */
  @Override
  public boolean isSameParent(Node node1, Node node2) {
    if (node1 == null || node2 == null) {
      return false;
    }
    netlock.readLock().lock();
    try {
      node1 = node1.getParent();
      node2 = node2.getParent();
      return Objects.equals(node1, node2);
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Get the ancestor for node on generation <i>ancestorGen</i>.
   *
   * @param node the node to get ancestor
   * @param ancestorGen  the ancestor generation
   * @return the ancestor. If no ancestor is found, then null is returned.
   */
  @Override
  public Node getAncestor(Node node, int ancestorGen) {
    if (node == null) {
      return null;
    }
    netlock.readLock().lock();
    try {
      return node.getAncestor(ancestorGen);
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Given a string representation of a node(leaf or inner), return its
   * reference.
   * @param loc a path string representing a node, can be leaf or inner node
   * @return a reference to the node, null if the node is not in the tree
   */
  @Override
  public Node getNode(String loc) {
    loc = NetUtils.normalize(loc);
    netlock.readLock().lock();
    try {
      if (!ROOT.equals(loc)) {
        return clusterTree.getNode(loc);
      } else {
        return clusterTree;
      }
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Given a string representation of Node, return its leaf nodes count.
   * @param loc a path-like string representation of Node
   * @return the number of leaf nodes for InnerNode, 1 for leaf node, 0 if node
   * doesn't exist
   */
  @Override
  public int getNumOfLeafNode(String loc) {
    netlock.readLock().lock();
    try {
      Node node = getNode(loc);
      if (node != null) {
        return node.getNumOfLeaves();
      }
    } finally {
      netlock.readLock().unlock();
    }
    return 0;
  }

  /**
   * Return the max level of this tree, start from 1 for ROOT. For example,
   * topology like "/rack/node" has the max level '3'.
   */
  @Override
  public int getMaxLevel() {
    return maxLevel;
  }

  static void assertLevel(int level, int max) {
    Preconditions.assertTrue(level > 0, () -> "level = " + level + " <= 0");
    Preconditions.assertTrue(level <= max, () -> "level = " + level + " > max = " + max);
  }

  /**
   * Return the node numbers at level <i>level</i>.
   * @param level topology level, start from 1, which means ROOT
   * @return the number of nodes on the level
   */
  @Override
  public int getNumOfNodes(int level) {
    assertLevel(level, getMaxLevel());
    netlock.readLock().lock();
    try {
      return clusterTree.getNumOfNodes(level);
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Return the node at level <i>level</i>.
   * @param level topology level, start from 1, which means ROOT
   * @return the nodes on the level
   */
  @Override
  public List<Node> getNodes(int level) {
    assertLevel(level, getMaxLevel());
    netlock.readLock().lock();
    try {
      return clusterTree.getNodes(level);
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Randomly choose a node in the scope.
   * @param scope range of nodes from which a node will be chosen. If scope
   *              starts with ~, choose one from the all nodes except for the
   *              ones in <i>scope</i>; otherwise, choose one from <i>scope</i>.
   * @return the chosen node
   */
  @Override
  public Node chooseRandom(String scope) {
    if (scope == null) {
      scope = ROOT;
    }
    if (scope.startsWith(SCOPE_REVERSE_STR)) {
      ArrayList<String> excludedScopes = new ArrayList<>();
      excludedScopes.add(scope.substring(1));
      return chooseRandom(ROOT, excludedScopes, null, null,
          ANCESTOR_GENERATION_DEFAULT);
    } else {
      return chooseRandom(scope, null, null, null, ANCESTOR_GENERATION_DEFAULT);
    }
  }

  /**
   * Randomly choose a node in the scope, ano not in the exclude scope.
   * @param scope range of nodes from which a node will be chosen. cannot start
   *              with ~
   * @param excludedScopes the chosen node cannot be in these ranges. cannot
   *                      starts with ~
   * @return the chosen node
   */
  @Override
  public Node chooseRandom(String scope, List<String> excludedScopes) {
    return chooseRandom(scope, excludedScopes, null, null,
        ANCESTOR_GENERATION_DEFAULT);
  }

  /**
   * Randomly choose a leaf node from <i>scope</i>.
   *
   * If scope starts with ~, choose one from the all nodes except for the
   * ones in <i>scope</i>; otherwise, choose nodes from <i>scope</i>.
   * If excludedNodes is given, choose a node that's not in excludedNodes.
   *
   * @param scope range of nodes from which a node will be chosen
   * @param excludedNodes nodes to be excluded
   *
   * @return the chosen node
   */
  @Override
  public Node chooseRandom(String scope, Collection<Node> excludedNodes) {
    if (scope == null) {
      scope = ROOT;
    }
    if (scope.startsWith(SCOPE_REVERSE_STR)) {
      ArrayList<String> excludedScopes = new ArrayList<>();
      excludedScopes.add(scope.substring(1));
      return chooseRandom(ROOT, excludedScopes, excludedNodes, null,
          ANCESTOR_GENERATION_DEFAULT);
    } else {
      return chooseRandom(scope, null, excludedNodes, null,
          ANCESTOR_GENERATION_DEFAULT);
    }
  }

  /**
   * Randomly choose a leaf node from <i>scope</i>.
   *
   * If scope starts with ~, choose one from the all nodes except for the
   * ones in <i>scope</i>; otherwise, choose nodes from <i>scope</i>.
   * If excludedNodes is given, choose a node that's not in excludedNodes.
   *
   * @param scope range of nodes from which a node will be chosen
   * @param excludedNodes nodes to be excluded from.
   * @param ancestorGen matters when excludeNodes is not null. It means the
   * ancestor generation that's not allowed to share between chosen node and the
   * excludedNodes. For example, if ancestorGen is 1, means chosen node
   * cannot share the same parent with excludeNodes. If value is 2, cannot
   * share the same grand parent, and so on. If ancestorGen is 0, then no
   * effect.
   *
   * @return the chosen node
   */
  @Override
  public Node chooseRandom(String scope, Collection<Node> excludedNodes,
      int ancestorGen) {
    if (scope == null) {
      scope = ROOT;
    }
    if (scope.startsWith(SCOPE_REVERSE_STR)) {
      ArrayList<String> excludedScopes = new ArrayList<>();
      excludedScopes.add(scope.substring(1));
      return chooseRandom(ROOT, excludedScopes, excludedNodes, null,
          ancestorGen);
    } else {
      return chooseRandom(scope, null, excludedNodes, null, ancestorGen);
    }
  }

  /**
   * Randomly choose one leaf node from <i>scope</i>, share the same generation
   * ancestor with <i>affinityNode</i>, and exclude nodes in
   * <i>excludeScope</i> and <i>excludeNodes</i>.
   *
   * @param scope range of nodes from which a node will be chosen, cannot start
   *              with ~
   * @param excludedScopes ranges of nodes to be excluded, cannot start with ~
   * @param excludedNodes nodes to be excluded
   * @param affinityNode  when not null, the chosen node should share the same
   *                     ancestor with this node at generation ancestorGen.
   *                      Ignored when value is null
   * @param ancestorGen If 0, then no same generation ancestor enforcement on
   *                     both excludedNodes and affinityNode. If greater than 0,
   *                     then apply to affinityNode(if not null), or apply to
   *                     excludedNodes if affinityNode is null
   * @return the chosen node
   */
  @Override
  public Node chooseRandom(String scope, List<String> excludedScopes,
      Collection<? extends Node> excludedNodes, Node affinityNode,
      int ancestorGen) {
    if (scope == null) {
      scope = ROOT;
    }

    checkScope(scope);
    checkExcludedScopes(excludedScopes);
    checkAffinityNode(affinityNode);
    checkAncestorGen(ancestorGen);

    netlock.readLock().lock();
    try {
      return chooseNodeInternal(scope, -1, excludedScopes,
          excludedNodes, affinityNode, ancestorGen);
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * Choose the leaf node at index <i>index</i> from <i>scope</i>, share the
   * same generation ancestor with <i>affinityNode</i>, and exclude nodes in
   * <i>excludeScope</i> and <i>excludeNodes</i>.
   *
   * @param leafIndex node index, exclude nodes in excludedScope and
   *                  excludedNodes
   * @param scope range of nodes from which a node will be chosen, cannot start
   *              with ~
   * @param excludedScopes ranges of nodes to be excluded, cannot start with ~
   * @param excludedNodes nodes to be excluded
   * @param affinityNode  when not null, the chosen node should share the same
   *                     ancestor with this node at generation ancestorGen.
   *                      Ignored when value is null
   * @param ancestorGen If 0, then no same generation ancestor enforcement on
   *                     both excludedNodes and affinityNode. If greater than 0,
   *                     then apply to affinityNode(if not null), or apply to
   *                     excludedNodes if affinityNode is null
   * @return the chosen node
   * Example:
   *
   *                                /  --- root
   *                              /  \
   *                             /    \
   *                            /      \
   *                           /        \
   *                         dc1         dc2
   *                        / \         / \
   *                       /   \       /   \
   *                      /     \     /     \
   *                    rack1 rack2  rack1  rack2
   *                   / \     / \  / \     / \
   *                 n1  n2  n3 n4 n5  n6  n7 n8
   *
   *   Input:
   *   leafIndex = 1
   *   excludedScope = /dc2
   *   excludedNodes = {/dc1/rack1/n1}
   *   affinityNode = /dc1/rack2/n2
   *   ancestorGen = 2
   *
   *   Output:
   *   node /dc1/rack2/n4
   *
   *   Explanation:
   *   With affinityNode n2 and ancestorGen 2, it means we can only pick node
   *   from subtree /dc1. LeafIndex 1, so we pick the 2nd available node n4.
   *
   */
  @Override
  public Node getNode(int leafIndex, String scope, List<String> excludedScopes,
      Collection<Node> excludedNodes, Node affinityNode, int ancestorGen) {
    Preconditions.assertTrue(leafIndex >= 0, () -> "leafIndex = " + leafIndex + " < 0");
    if (scope == null) {
      scope = ROOT;
    }
    checkScope(scope);
    checkExcludedScopes(excludedScopes);
    checkAffinityNode(affinityNode);
    checkAncestorGen(ancestorGen);

    netlock.readLock().lock();
    try {
      return chooseNodeInternal(scope, leafIndex, excludedScopes,
          excludedNodes, affinityNode, ancestorGen);
    } finally {
      netlock.readLock().unlock();
    }
  }

  @SuppressWarnings("java:S2245") // no need for secure random
  private Node chooseNodeInternal(String scope, int leafIndex,
      List<String> excludedScopes, Collection<? extends Node> excludedNodes,
      Node affinityNode, int ancestorGen) {
    Objects.requireNonNull(scope, "scope == null");
    if (LOG.isDebugEnabled()) {
      LOG.debug("Start choosing node[scope = {}, index = {}, excludedScopes = "
              + "{}, excludedNodes = {}, affinityNode = {}, ancestorGen = {}",
          scope, leafIndex,
          excludedScopes == null ? "" : excludedScopes,
          excludedNodes == null ? "" : excludedNodes,
          affinityNode == null ? "" : affinityNode, ancestorGen);
    }

    String finalScope = scope;
    if (affinityNode != null && ancestorGen > 0) {
      Node affinityAncestor = affinityNode.getAncestor(ancestorGen);
      if (affinityAncestor == null) {
        throw new IllegalArgumentException("affinityNode " +
            affinityNode.getNetworkFullPath() + " doesn't have ancestor on" +
            " generation  " + ancestorGen);
      }
      // affinity ancestor should has overlap with scope
      if (affinityAncestor.isDescendant(scope)) {
        finalScope = affinityAncestor.getNetworkFullPath();
      } else if (!scope.startsWith(affinityAncestor.getNetworkFullPath())) {
        return null;
      }
      // reset ancestor generation since the new scope is identified now
      ancestorGen = 0;
    }
    Node scopeNode = getNode(finalScope);
    if (scopeNode == null) {
      throw new IllegalArgumentException(String.format("No nodes with Scope: " +
              "%s exists", finalScope));
    }

    // check overlap of excludedScopes and finalScope
    List<String> mutableExcludedScopes = null;
    if (excludedScopes != null && !excludedScopes.isEmpty()) {
      mutableExcludedScopes = new ArrayList<>();
      for (String s: excludedScopes) {
        // excludeScope covers finalScope
        if (scopeNode.isDescendant(s)) {
          return null;
        }
        // excludeScope and finalScope share nothing case
        if (scopeNode.isAncestor(s)) {
          Node node = getNode(s);
          if (node != null &&
              mutableExcludedScopes.stream().noneMatch(node::isDescendant)) {
            mutableExcludedScopes.add(s);
          }
        }
      }
    }

    // clone excludedNodes before remove duplicate in it
    Collection<Node> mutableExNodes = new LinkedHashSet<>();

    // add affinity node to mutableExNodes
    if (affinityNode != null) {
      mutableExNodes.add(affinityNode);
    }

    // Remove duplicate in excludedNodes
    if (excludedNodes != null) {
      mutableExNodes.addAll(excludedNodes);
    }

    // remove duplicate in mutableExNodes and mutableExcludedScopes
    NetUtils.removeDuplicate(this, mutableExNodes, mutableExcludedScopes,
        ancestorGen);

    // calculate available node count
    int availableNodes = getAvailableNodesCount(
        scopeNode.getNetworkFullPath(), mutableExcludedScopes, mutableExNodes,
        ancestorGen);

    if (availableNodes <= 0) {
      LOG.info("No available node in (scope=\"{}\" excludedScope=\"{}\" " +
              "excludedNodes=\"{}\"  ancestorGen=\"{}\").",
          scopeNode.getNetworkFullPath(), excludedScopes, excludedNodes,
          ancestorGen);
      return null;
    }

    // scope is a Leaf node
    if (!(scopeNode instanceof InnerNode)) {
      return scopeNode;
    }

    Node ret;
    int nodeIndex;
    if (leafIndex >= 0) {
      nodeIndex = leafIndex % availableNodes;
      ret = ((InnerNode)scopeNode).getLeaf(nodeIndex, mutableExcludedScopes,
          mutableExNodes, ancestorGen);
    } else {
      nodeIndex = ThreadLocalRandom.current().nextInt(availableNodes);
      ret = ((InnerNode)scopeNode).getLeaf(nodeIndex, mutableExcludedScopes,
          mutableExNodes, ancestorGen);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Finish choosing node[index = {}, random = {}] from {} " +
              "available nodes, scope = {}, excludedScope = {}," +
              "excludeNodes = {}.",
          nodeIndex, (leafIndex == -1 ? "true" : "false"), availableNodes,
          scopeNode.getNetworkFullPath(), excludedScopes, excludedNodes);
      LOG.debug("Chosen node = {}", (ret == null ? "not found" :
          ret.toString()));
    }
    return ret;
  }

  /** Return the distance cost between two nodes
   * The distance cost from one node to its parent is it's parent's cost
   * The distance cost between two nodes is calculated by summing up their
   * distances cost to their closest common ancestor.
   * @param node1 one node
   * @param node2 another node
   * @return the distance cost between node1 and node2 which is zero if they
   * are the same or {@link Integer#MAX_VALUE} if node1 or node2 do not belong
   * to the cluster
   */
  @Override
  public int getDistanceCost(Node node1, Node node2) {
    if (Objects.equals(node1, node2)) {
      return 0;
    }
    if (node1 == null || node2 == null) {
      LOG.warn("One of the nodes is a null pointer");
      return Integer.MAX_VALUE;
    }

    // verify levels are in range
    int level1 = node1.getLevel();
    int level2 = node2.getLevel();
    if (level1 < NetConstants.ROOT_LEVEL || level2 < NetConstants.ROOT_LEVEL) {
      return Integer.MAX_VALUE;
    }
    if (level1 > maxLevel || level2 > maxLevel) {
      return Integer.MAX_VALUE;
    }

    int cost = 0;
    netlock.readLock().lock();
    try {
      Node ancestor1 = node1.getAncestor(level1 - 1);
      Node ancestor2 = node2.getAncestor(level2 - 1);
      if (!Objects.equals(ancestor1, clusterTree) ||
          !Objects.equals(ancestor2, clusterTree)) {
        LOG.debug("One of the nodes is outside of network topology");
        return Integer.MAX_VALUE;
      }
      while (level1 > level2 && node1 != null) {
        node1 = node1.getParent();
        level1--;
        cost += node1 == null ? 0 : node1.getCost();
      }
      while (level2 > level1 && node2 != null) {
        node2 = node2.getParent();
        level2--;
        cost += node2 == null ? 0 : node2.getCost();
      }
      while (node1 != null && node2 != null && !Objects.equals(node1, node2)) {
        node1 = node1.getParent();
        node2 = node2.getParent();
        cost += node1 == null ? 0 : node1.getCost();
        cost += node2 == null ? 0 : node2.getCost();
      }
      return cost;
    } finally {
      netlock.readLock().unlock();
    }
  }

  static class DistanceMap<E> {
    private final Function<E, Node> getNode;
    private final Map<String, Integer> map = new HashMap<>();

    DistanceMap(Function<E, Node> getNode) {
      this.getNode = getNode;
    }

    private String key(E element) {
      return getNode.apply(element).getNetworkFullPath();
    }

    int getDistance(E element) {
      return map.get(key(element));
    }

    void putNonExisting(E element, int distance) {
      final Integer previous = map.put(key(element), distance);
      Preconditions.assertNull(previous, "previous");
    }
  }

  @Override
  public <E> void sortByDistance(List<E> elements, Function<E, Node> getNode, Node client) {
    if (elements.isEmpty()) {
      return;
    }

    shuffleOperation.accept(elements);
    if (client == null) {
      move(elements, getNode, e -> 1);
      return;
    }

    // init distance map
    final DistanceMap<E> map = new DistanceMap<>(getNode);
    for (E e : elements) {
      final Node node = getNode.apply(e);
      final int distance = getDistanceCost(client, node);
      map.putNonExisting(e, distance);
    }

    // sort and move
    elements.sort(Comparator.comparingInt(map::getDistance));
    move(elements, getNode, map::getDistance);
  }

  /** Move the elements with the same distance and parent next to each other. */
  static <E> void move(List<E> elements, Function<E, Node> getNode, ToIntFunction<E> getDistance) {
    for (int i = 0; i < elements.size() - 2; i++) {
      final E current = elements.get(i);
      final int distance = getDistance.applyAsInt(current);
      final InnerNode parent = getNode.apply(current).getParent();

      // check if the next element should be swapped
      final int fromIndex = i + 1;
      final E from = elements.get(i + 1);
      if (getDistance.applyAsInt(from) != distance) {
        // different distance, nothing to swap
        continue;
      }
      if (getNode.apply(from).getParent() == parent) {
        // same parent, nothing to swap
        continue;
      }

      // find another element with the same distance and the same parent
      E to = null;
      int toIndex = i + 2;
      for (; toIndex < elements.size(); toIndex++) {
        final E e = elements.get(toIndex);
        if (getDistance.applyAsInt(to) != distance) {
          // different distance, nothing to swap
          break;
        }
        if (getNode.apply(from).getParent() == parent) {
          // same parent, should swap to it
          to = e;
          break;
        }
      }

      // swap
      if (to != null) {
        elements.set(fromIndex, to);
        elements.set(toIndex, from);
      }
    }
  }

  /**
   * Return the number of leaves in <i>scope</i> but not in
   * <i>excludedNodes</i> and <i>excludeScope</i>.
   * @param scope the scope
   * @param excludedScopes excluded scopes
   * @param mutableExcludedNodes a list of excluded nodes, content might be
   *                            changed after the call
   * @param ancestorGen same generation ancestor prohibit on excludedNodes
   * @return number of available nodes
   */
  private int getAvailableNodesCount(String scope, List<String> excludedScopes,
      Collection<Node> mutableExcludedNodes, int ancestorGen) {
    Objects.requireNonNull(scope, "scope == null");

    Node scopeNode = getNode(scope);
    if (scopeNode == null) {
      return 0;
    }
    if (!CollectionUtils.isEmpty(mutableExcludedNodes)) {
      mutableExcludedNodes.removeIf(next -> !next.isDescendant(scope));
    }
    List<Node> excludedAncestorList =
        NetUtils.getAncestorList(this, mutableExcludedNodes, ancestorGen);
    for (Node ancestor : excludedAncestorList) {
      if (ancestor.isAncestor(scope)) {
        return 0;
      }
    }
    // number of nodes to exclude
    int excludedCount = 0;
    if (excludedScopes != null) {
      for (String excludedScope: excludedScopes) {
        Node excludedScopeNode = getNode(excludedScope);
        if (excludedScopeNode != null) {
          if (excludedScopeNode.isDescendant(scope)) {
            excludedCount += excludedScopeNode.getNumOfLeaves();
          } else if (excludedScopeNode.isAncestor(scope)) {
            return 0;
          }
        }
      }
    }
    // excludedNodes is not null case
    if (!CollectionUtils.isEmpty(mutableExcludedNodes)) {
      if (ancestorGen == 0) {
        for (Node node: mutableExcludedNodes) {
          if (contains(node)) {
            excludedCount++;
          }
        }
      } else {
        for (Node ancestor : excludedAncestorList) {
          if (ancestor.isDescendant(scope)) {
            excludedCount += ancestor.getNumOfLeaves();
          }
        }
      }
    }

    int availableCount = scopeNode.getNumOfLeaves() - excludedCount;
    Preconditions.assertTrue(availableCount >= 0, () -> "availableCount = " + availableCount + " < 0");
    return availableCount;
  }

  @Override
  public String toString() {
    // print max level
    StringBuilder tree = new StringBuilder();
    tree.append("Level: ")
        .append(maxLevel)
        .append('\n');
    netlock.readLock().lock();
    try {
      // print the number of leaves
      int numOfLeaves = clusterTree.getNumOfLeaves();
      tree.append("Number of leaves:")
          .append(numOfLeaves)
          .append('\n');
      // print all nodes
      for (int i = 0; i < numOfLeaves; i++) {
        tree.append(clusterTree.getLeaf(i).getNetworkFullPath())
            .append('\n');
      }
    } finally {
      netlock.readLock().unlock();
    }
    return tree.toString();
  }

  private void checkScope(String scope) {
    if (scope != null && scope.startsWith(SCOPE_REVERSE_STR)) {
      throw new IllegalArgumentException("scope " + scope +
          " should not start with " + SCOPE_REVERSE_STR);
    }
  }

  private void checkExcludedScopes(List<String> excludedScopes) {
    if (!CollectionUtils.isEmpty(excludedScopes)) {
      excludedScopes.forEach(scope -> {
        if (scope.startsWith(SCOPE_REVERSE_STR)) {
          throw new IllegalArgumentException("excludedScope " + scope +
              " cannot start with " + SCOPE_REVERSE_STR);
        }
      });
    }
  }

  private void checkAffinityNode(Node affinityNode) {
    if (affinityNode != null && (!contains(affinityNode))) {
      throw new IllegalArgumentException("Affinity node " +
          affinityNode.getNetworkFullPath() + " is not a member of topology");
    }
  }

  private void checkAncestorGen(int ancestorGen) {
    if (ancestorGen > (maxLevel - 1) || ancestorGen < 0) {
      throw new IllegalArgumentException("ancestorGen " + ancestorGen +
          " exceeds this network topology acceptable level [0, " +
          (maxLevel - 1) + "]");
    }
  }
}
