package fj.model.persistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import fj.model.pojos.Jaccard;
import fj.model.pojos.PNodeId;
import fj.model.pojos.PNodeInfo;
import fj.model.pojos.PNodeState;
import fj.model.pojos.PpEdge;
import fj.model.pojos.SNodeId;
import org.apache.fluo.api.client.SnapshotBase;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.recipes.core.types.TypedTransactionBase;

public class PNodePersistence {

  private static final String PNODE_PREFIX = "p:";
  public static final Column PNODE_NTFY_COL = new Column("pnode", "updated");
  private static final String NEW_INTERSECTION_PREFIX = "nIsec:";
  private static final String HIGH_NEIGHBOR_FAM = "hPair";
  private static final String LOW_NEIGHBOR_FAM = "lPair";
  private static final Column DEGREE_COL = new Column("node", "dgree");
  private static final String NEW_DEGREE_FAM = "nDeg";

  // TODO transition fomr Bytes to String if it makes code easier to understand.
  // TODO could move to keeping everying known about another PNode in a single value

  public static PNodeState getState(SnapshotBase tx, PNodeId pnode) {

    HashMap<PNodeId, Integer> degreeUpdates = new HashMap<>();
    HashMap<PNodeId, List<SNodeId>> intersectionUpdates = new HashMap<>();
    Set<SNodeId> newSNodes = new HashSet<>();
    HashMap<PNodeId, PNodeInfo> highNeighbors = new HashMap<>();
    Set<PNodeId> lowNeighbors = new HashSet<>();
    int degree = 0;

    CellScanner scanner = tx.scanner().over(Span.exact(toRow(pnode))).build();
    for (RowColumnValue rcv : scanner) {
      Column col = rcv.getColumn();
      String fam = col.getsFamily();
      String qual = col.getsQualifier();
      String val = rcv.getsValue();

      if (fam.equals(PNodePersistence.HIGH_NEIGHBOR_FAM)) {
        String[] fields = val.toString().split(" ");
        int intersection = Integer.parseInt(fields[0]);
        int otherDegree = Integer.parseInt(fields[1]);

        Jaccard exportedJaccard = null;
        if (fields.length == 4) {
          exportedJaccard = new Jaccard(Integer.parseInt(fields[2]), Integer.parseInt(fields[3]));
        }

        PNodeInfo pInfo = new PNodeInfo(intersection, otherDegree, exportedJaccard);
        highNeighbors.put(new PNodeId(qual), pInfo);
      } else if (fam.equals(PNodePersistence.LOW_NEIGHBOR_FAM)) {
        lowNeighbors.add(new PNodeId(qual));
      } else if (fam.equals(PNodePersistence.NEW_DEGREE_FAM)) {
        degreeUpdates.put(new PNodeId(qual), Integer.valueOf(val.toString()));
      } else if (fam.equals(SNodePersistence.NEW_EDGE_FAM)) {
        newSNodes.add(new SNodeId(qual));
      } else if (fam.startsWith(PNodePersistence.NEW_INTERSECTION_PREFIX)) {
        String suffix = fam.substring(PNodePersistence.NEW_INTERSECTION_PREFIX.length(), fam.length());
        List<SNodeId> snodes =
            intersectionUpdates.computeIfAbsent(new PNodeId(qual), k -> new ArrayList<>());
        snodes.add(new SNodeId(suffix));
      } else if (col.equals(PNodePersistence.DEGREE_COL)) {
        degree = Integer.parseInt(val);
      }
    }

    return new PNodeState(degree, highNeighbors, lowNeighbors, degreeUpdates, intersectionUpdates,
        newSNodes);
  }

  public static void queueDegreeUpdates(TypedTransactionBase tx, PNodeId pnode, int degree,
      Set<PNodeId> lowNeighbors) {
    for (PNodeId pnid : lowNeighbors) {
      tx.mutate().row(toRow(pnid)).fam(PNodePersistence.NEW_DEGREE_FAM).qual(pnode.getId()).set(degree);
      tx.mutate().row(toRow(pnid)).col(PNodePersistence.PNODE_NTFY_COL).weaklyNotify();
    }
  }

  public static void deleteUpdates(TypedTransactionBase tx, PNodeId pnode, PNodeState state) {
    String row = toRow(pnode);

    for (SNodeId snode : state.getNewSNodes()) {
      tx.mutate().row(row).fam(SNodePersistence.NEW_EDGE_FAM).qual(snode.getId()).delete();
    }

    for (PNodeId pnid : state.getDegreeUpdates().keySet()) {
      // TODO could cause collisions.. could leave and only react to when it differs from known val
      //tx.mutate().row(row).fam(PNodePersistence.NEW_DEGREE_FAM).qual(pnid.getId()).delete();
    }

    for (Entry<PNodeId, List<SNodeId>> entry : state.getIntersectionUpdates().entrySet()) {
      PNodeId pnid = entry.getKey();
      for (SNodeId snid : entry.getValue()) {
        String fam = PNodePersistence.NEW_INTERSECTION_PREFIX + snid.toString();
        tx.mutate().row(row).fam(fam).qual(pnid.getId()).delete();
      }
    }
  }

  public static void updateHighNeighbors(TypedTransactionBase tx, PNodeId pnode,
      Map<PNodeId, PNodeInfo> hnUpdates) {
    String row = toRow(pnode);

    for (Entry<PNodeId, PNodeInfo> entry : hnUpdates.entrySet()) {
      PNodeId pNodeId = entry.getKey();
      PNodeInfo pnInfo = entry.getValue();
      Jaccard ej = pnInfo.getExportedJaccard();

      String newVal = pnInfo.getIntersection() + " " + pnInfo.getDegree();
      if (ej != null) {
        newVal += " " + ej.getIntersection() + " " + ej.getUnion();
      }

      tx.mutate().row(row).fam(PNodePersistence.HIGH_NEIGHBOR_FAM).qual(pNodeId.getId()).set(newVal);
    }
  }

  public static void addLowNeighbors(TypedTransactionBase tx, PNodeId pnode, Set<PNodeId> updates) {
    String row = toRow(pnode);
    for (PNodeId pnid : updates) {
      tx.mutate().row(row).fam(PNodePersistence.LOW_NEIGHBOR_FAM).qual(pnid.getId()).set();
    }
  }

  public static void queueNewEdges(TypedTransactionBase tx, SNodeId snode, Set<PpEdge> newEdges) {
    String family = PNodePersistence.NEW_INTERSECTION_PREFIX + snode.getId();

    for (PpEdge ppEdge : newEdges) {
      PNodeId pn1 = ppEdge.getNode1();
      PNodeId pn2 = ppEdge.getNode2();
      tx.mutate().row(PNodePersistence.toRow(pn1)).fam(family).qual(pn2.getId()).set();
      tx.mutate().row(PNodePersistence.toRow(pn2)).fam(family).qual(pn1.getId()).set();
    }
  }

  public static void updateDegree(TypedTransactionBase tx, PNodeId pnode, int degree) {
    tx.mutate().row(toRow(pnode)).col(PNodePersistence.DEGREE_COL).set(degree);
  }

  public static void queueDegreeUpdates(TypedTransactionBase tx, SNodeId snode,
      Set<PNodeId> newNeigbors) {
    for (PNodeId pnid : newNeigbors) {
      tx.mutate().row(toRow(pnid)).fam(SNodePersistence.NEW_EDGE_FAM).qual(snode.getId()).set();
    }
  }

  public static void notifyObservers(TypedTransactionBase tx, Iterable<PNodeId> changedNodes) {
    for (PNodeId pnid : changedNodes) {
      tx.mutate().row(toRow(pnid)).col(PNodePersistence.PNODE_NTFY_COL).weaklyNotify();
    }
  }

  public static PNodeId fromRow(Bytes row) {
    return new PNodeId(row.subSequence(PNodePersistence.PNODE_PREFIX.length(), row.length()).toString());
  }

  public static String toRow(PNodeId pNodeId) {
    return PNodePersistence.PNODE_PREFIX + pNodeId.getId();
  }
}
