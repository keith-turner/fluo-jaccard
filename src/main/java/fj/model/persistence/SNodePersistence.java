package fj.model.persistence;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import fj.model.pojos.PNodeId;
import fj.model.pojos.PsEdge;
import fj.model.pojos.SNodeId;
import fj.model.pojos.SNodeState;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.recipes.core.types.TypedTransactionBase;

public class SNodePersistence {

  public static final String SNODE_PREFIX = "s:";
  public static final Column SNODE_NTFY_COL = new Column("snode", "updated");
  public static final String EDGE_FAM = "edge";
  public static final String NEW_EDGE_FAM = "nEdge";

  public static void incorporateNewNeighbors(TypedTransactionBase tx, SNodeId snode,
      SNodeState state) {
    for (PNodeId pnid : state.getNewNeighbors()) {
      tx.mutate().row(toRow(snode)).fam(SNodePersistence.EDGE_FAM).qual(pnid.getId()).set();
      tx.mutate().row(toRow(snode)).fam(SNodePersistence.NEW_EDGE_FAM).qual(pnid.getId()).delete();
    }
  }

  public static SNodeState getState(TypedTransactionBase tx, SNodeId nodeId) {

    HashSet<PNodeId> neighbors = new HashSet<>();
    HashSet<PNodeId> newNeighbors = new HashSet<>();

    CellScanner scanner = tx.scanner().over(Span.exact(toRow(nodeId))).build();
    for (RowColumnValue rcv : scanner) {
      Column col = rcv.getColumn();
      String fam = col.getsFamily();
      String qual = col.getsQualifier();

      if (fam.equals(SNodePersistence.EDGE_FAM)) {
        neighbors.add(new PNodeId(qual));
      } else if (fam.equals(SNodePersistence.NEW_EDGE_FAM)) {
        newNeighbors.add(new PNodeId(qual));
      }
    }

    return new SNodeState(neighbors, newNeighbors);
  }

  public static Set<PsEdge> lookupEdges(TypedTransactionBase tx, Set<PsEdge> edges) {

    Set<PsEdge> existingEdges = new HashSet<>();
    Map<SNodeId, Set<PNodeId>> groupedEdges = new HashMap<>();

    // group edges that are in the same row
    for (PsEdge psEdge : edges) {
      groupedEdges.computeIfAbsent(psEdge.getNode2(), k -> new HashSet<>()).add(psEdge.getNode1());
    }


    for (Entry<SNodeId, Set<PNodeId>> entry : groupedEdges.entrySet()) {
      HashSet<Column> columns = new HashSet<>();
      for (PNodeId pnid : entry.getValue()) {
        columns.add(new Column(SNodePersistence.NEW_EDGE_FAM, pnid.getId()));
        columns.add(new Column(SNodePersistence.EDGE_FAM, pnid.getId()));
      }

      // TODO fluo needs a primitive operations to fetch a list of ranges/spans... then this could
      // be done with one call
      // find existing edges.. rather than a call per row
      Set<Column> found = tx.get().row(toRow(entry.getKey())).columns(columns).keySet();
      for (Column column : found) {
        String qual = column.getQualifier().toString();
        existingEdges.add(new PsEdge(new PNodeId(qual), entry.getKey()));
      }
    }

    return existingEdges;
  }

  public static void addEdges(TypedTransactionBase tx, Set<PsEdge> newEdges) {
    HashSet<String> rows = new HashSet<>();
    for (PsEdge psEdge : newEdges) {
      String snodeRow = toRow(psEdge.getNode2());
      tx.mutate().row(snodeRow).fam(SNodePersistence.NEW_EDGE_FAM).qual(psEdge.getNode1().getId()).set();
      rows.add(snodeRow);
    }

    for (String row : rows) {
      tx.mutate().row(row).col(SNodePersistence.SNODE_NTFY_COL).weaklyNotify();
    }
  }

  public static String toRow(SNodeId sNodeId) {
    return SNodePersistence.SNODE_PREFIX + sNodeId.getId();
  }

  public static SNodeId fromRow(Bytes row) {
    return new SNodeId(row.subSequence(SNODE_PREFIX.length(), row.length()).toString());
  }
}
