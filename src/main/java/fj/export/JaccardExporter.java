package fj.export;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

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
  protected Collection<Mutation> translate(SequencedExport<PpEdge, JaccardExport> se) {
    return AccumuloExporter.generateMutations(se.getSequence(),
        generateData(se.getKey(), se.getValue().getOldVal()),
        generateData(se.getKey(), se.getValue().getNewVal()));

  }
}
