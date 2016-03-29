package fj.model.pojos;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class PNodeState {

  private Map<PNodeId, PNodeInfo> highNeighbors;
  private Set<PNodeId> lowNeighbors;
  private Map<PNodeId, Integer> degreeUpdates;
  private Map<PNodeId, List<SNodeId>> intersectionUpdates;
  private Set<SNodeId> newSNodes;
  private int degree;


  public PNodeState(int degree, Map<PNodeId, PNodeInfo> highNeighbors, Set<PNodeId> lowNeighbors,
      Map<PNodeId, Integer> degreeUpdates, Map<PNodeId, List<SNodeId>> intersectionUpdates,
      Set<SNodeId> newSNodes) {
    this.degree = degree;
    this.highNeighbors = highNeighbors;
    this.lowNeighbors = lowNeighbors;
    this.degreeUpdates = degreeUpdates;
    this.intersectionUpdates = intersectionUpdates;
    this.newSNodes = newSNodes;
  }

  public int getDegreeChange() {
    return newSNodes.size();
  }

  public int getDegree() {
    return degree + newSNodes.size();
  }

  public Map<PNodeId, Integer> getDegreeUpdates() {
    return degreeUpdates;
  }

  public Map<PNodeId, List<SNodeId>> getIntersectionUpdates() {
    return intersectionUpdates;
  }

  public Set<SNodeId> getNewSNodes() {
    return newSNodes;
  }

  public Map<PNodeId, PNodeInfo> getHighNeighbors() {
    return highNeighbors;
  }

  public Set<PNodeId> getLowNeighbors() {
    return lowNeighbors;
  }
}
