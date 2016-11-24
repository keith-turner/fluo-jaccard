package fj.logic;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import fj.Application;
import fj.model.persistence.PNodePersistence;
import fj.model.pojos.Jaccard;
import fj.model.pojos.JaccardExport;
import fj.model.pojos.PNodeId;
import fj.model.pojos.PNodeInfo;
import fj.model.pojos.PNodeState;
import fj.model.pojos.PpEdge;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.recipes.core.export.ExportQueue;
import org.apache.fluo.recipes.core.types.TypedObserver;
import org.apache.fluo.recipes.core.types.TypedTransactionBase;

public class PNodeObserver extends TypedObserver {

  private ExportQueue<PpEdge, JaccardExport> exportQueue;


  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(PNodePersistence.PNODE_NTFY_COL, NotificationType.WEAK);
  }

  @Override
  public void init(Context context) throws Exception {
    exportQueue =
        ExportQueue.getInstance(Application.EXPORT_QUEUE_ID, context.getAppConfiguration());
  }

  @Override
  public void process(TypedTransactionBase tx, Bytes row, Column col) {
    PNodeId pnode = PNodePersistence.fromRow(row);
    PNodeState state = PNodePersistence.getState(tx, pnode);

    Set<PNodeId> newHigh = new HashSet<>();
    Set<PNodeId> newLow = new HashSet<>();

    for (PNodeId oPnid : state.getIntersectionUpdates().keySet()) {
      int cmp = oPnid.compareTo(pnode);
      if (cmp > 0 && !state.getHighNeighbors().containsKey(oPnid)) {
        newHigh.add(oPnid);
      } else if (cmp < 0 && !state.getLowNeighbors().contains(oPnid)) {
        newLow.add(oPnid);
      }
    }

    Map<PNodeId, PNodeInfo> highUpdates = new HashMap<>();

    for (PNodeId hPnid : Iterables.concat(newHigh, state.getHighNeighbors().keySet())) {
      PNodeInfo currentInfo = state.getHighNeighbors().getOrDefault(hPnid, PNodeInfo.ZERO);
      PNodeInfo newInfo = new PNodeInfo(currentInfo);

      Integer degreeUpdate = state.getDegreeUpdates().get(hPnid);
      if (degreeUpdate != null) {
        newInfo.setDegree(degreeUpdate);
      }

      int intersectionUpdate =
          state.getIntersectionUpdates().getOrDefault(hPnid, Collections.emptyList()).size();
      newInfo.incrementIntersection(intersectionUpdate);

      Jaccard currentJaccard = currentInfo.getExportedJaccard();
      Jaccard newJaccard =
          new Jaccard(newInfo.getIntersection(), state.getDegree(), newInfo.getDegree());
      if (newJaccard.isValid() && (currentJaccard == null || !currentJaccard.equals(newJaccard))) {
        newInfo.setExportedJaccard(newJaccard);
        export(tx, pnode, hPnid, currentJaccard, newJaccard);
      }

      if (!currentInfo.equals(newInfo)) {
        highUpdates.put(hPnid, newInfo);
      }
    }

    if (state.getDegreeChange() > 0) {
      PNodePersistence.updateDegree(tx, pnode, state.getDegree());
      PNodePersistence.queueDegreeUpdates(tx, pnode, state.getDegree(),
          Sets.union(newLow, state.getLowNeighbors()));
    } else {
      // TODO comment out to show debugging
      PNodePersistence.queueDegreeUpdates(tx, pnode, state.getDegree(),
          Sets.difference(newLow, state.getLowNeighbors()));
    }

    PNodePersistence.updateHighNeighbors(tx, pnode, highUpdates);
    PNodePersistence.addLowNeighbors(tx, pnode, newLow);
    PNodePersistence.deleteUpdates(tx, pnode, state);
  }

  private void export(TypedTransactionBase tx, PNodeId pnode, PNodeId hPnid, Jaccard currentJaccard,
      Jaccard newJaccard) {
    exportQueue.add(tx, new PpEdge(pnode, hPnid), new JaccardExport(currentJaccard, newJaccard));
  }
}
