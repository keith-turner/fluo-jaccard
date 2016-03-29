package fj.model.pojos;

import java.util.Set;

/**
 * An immutable snapshot of the current state of an S-node.
 */
public class SNodeState {

  private Set<PNodeId> neighbors;
  private Set<PNodeId> newNeighbors;

  public SNodeState(Set<PNodeId> neighbors, Set<PNodeId> newNeighbors) {
    this.neighbors = neighbors;
    this.newNeighbors = newNeighbors;
  }

  public Set<PNodeId> getNewNeighbors() {
    return newNeighbors;
  }

  public Set<PNodeId> getNeighbors() {
    return neighbors;
  }
}
