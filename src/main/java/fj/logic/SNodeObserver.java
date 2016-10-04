package fj.logic;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Iterables;
import fj.model.persistence.PNodePersistence;
import fj.model.persistence.SNodePersistence;
import fj.model.pojos.PNodeId;
import fj.model.pojos.PpEdge;
import fj.model.pojos.SNodeId;
import fj.model.pojos.SNodeState;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.recipes.core.types.TypedObserver;
import org.apache.fluo.recipes.core.types.TypedTransactionBase;

public class SNodeObserver extends TypedObserver {

  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(SNodePersistence.SNODE_NTFY_COL, NotificationType.WEAK);
  }

  @Override
  public void process(TypedTransactionBase tx, Bytes row, Column col) {
    SNodeId snodeId = SNodePersistence.fromRow(row);
    SNodeState state = SNodePersistence.getState(tx, snodeId);

    // neighbors
    Set<PNodeId> neighbors = state.getNeighbors();
    Set<PNodeId> newNeigbors = state.getNewNeighbors();

    Set<PpEdge> newEdges = newCombinations(neighbors, newNeigbors);

    PNodePersistence.queueNewEdges(tx, snodeId, newEdges);
    PNodePersistence.queueDegreeUpdates(tx, snodeId, newNeigbors);
    PNodePersistence.notifyObservers(tx, Iterables.concat(neighbors, newNeigbors));

    SNodePersistence.incorporateNewNeighbors(tx, snodeId, state);
  }

  static Set<PpEdge> newCombinations(Collection<PNodeId> existingNodes,
      Collection<PNodeId> newNodes) {

    Set<PpEdge> newPairs = new HashSet<>();

    for (PNodeId newNeighbor : newNodes) {
      for (PNodeId neighbor : existingNodes) {
        int cmp = newNeighbor.compareTo(neighbor);
        if (cmp < 0) {
          newPairs.add(new PpEdge(newNeighbor, neighbor));
        } else if (cmp > 0) {
          newPairs.add(new PpEdge(neighbor, newNeighbor));
        }
      }
    }

    for (PNodeId newNeighbor1 : newNodes) {
      for (PNodeId newNeighbor2 : newNodes) {
        if (newNeighbor1.compareTo(newNeighbor2) < 0) {
          newPairs.add(new PpEdge(newNeighbor1, newNeighbor2));
        }
      }
    }
    return newPairs;
  }
}
