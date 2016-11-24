package fj.export;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;
import fj.model.pojos.JaccardExport;
import fj.model.pojos.PpEdge;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.recipes.accumulo.export.AccumuloExporter;
import org.apache.fluo.recipes.core.export.SequencedExport;
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.lexicoder.ReverseLexicoder;
import org.apache.accumulo.core.data.Mutation;
import org.apache.commons.codec.binary.Hex;

public class JaccardExporter extends AccumuloExporter<PpEdge, JaccardExport> {

  private static final ReverseLexicoder<Double> rl = new ReverseLexicoder<>(new DoubleLexicoder());

  public JaccardExporter() {}

  protected Map<RowColumn, Bytes> generateData(PpEdge key, Double jaccard) {
    if (jaccard == null) {
      return Collections.emptyMap();
    }

    Bytes node1 = Bytes.of(key.getNode1().getId());
    Bytes node2 = Bytes.of(key.getNode2().getId());
    Bytes fam = Bytes.of(rl.encode(jaccard));

    return ImmutableMap.of(new RowColumn(node1, new Column(fam, node2)), Bytes.EMPTY,
        new RowColumn(node2, new Column(fam, node1)), Bytes.EMPTY);
  }

  @Override
  protected void translate(SequencedExport<PpEdge, JaccardExport> se, Consumer<Mutation> output) {

    PpEdge edge = se.getKey();
    JaccardExport je = se.getValue();

    String id1 = edge.getNode1().getId();
    String id2 = edge.getNode2().getId();

    Mutation m1 = new Mutation("e:" + id1);
    Mutation m2 = new Mutation("e:" + id2);

    if (je.getOldVal() != null) {
      String encJaccard = Hex.encodeHexString(rl.encode(je.getOldVal()));
      m1.putDelete(encJaccard, id2, se.getSequence());
      m2.putDelete(encJaccard, id1, se.getSequence());

      // TODO big row
      Mutation m3 = new Mutation("t:" + encJaccard);
      m3.putDelete(id1, id2, se.getSequence());
      output.accept(m3);
    }

    if (je.getNewVal() != null) {
      String encJaccard = Hex.encodeHexString(rl.encode(je.getNewVal()));
      m1.put(encJaccard, id2, se.getSequence(), "");
      m2.put(encJaccard, id1, se.getSequence(), "");

      Mutation m4 = new Mutation("t:" + encJaccard);
      m4.put(id1, id2, se.getSequence(), "");
      output.accept(m4);
    }

    output.accept(m1);
    output.accept(m2);
  }
}
