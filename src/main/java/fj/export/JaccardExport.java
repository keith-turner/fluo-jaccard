package fj.export;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import fj.model.pojos.Jaccard;
import fj.model.pojos.PpEdge;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.recipes.accumulo.export.DifferenceExport;
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.lexicoder.ReverseLexicoder;

public class JaccardExport extends DifferenceExport<PpEdge, Jaccard> {

  private static final ReverseLexicoder<Double> rl = new ReverseLexicoder<>(new DoubleLexicoder());

  public JaccardExport(){}

  public JaccardExport(Optional<Jaccard> oldVal, Optional<Jaccard> newVal) {
    // TODO super type should probably avoid using Optional for serialization reasons
    super(oldVal, newVal);
  }

  @Override
  protected Map<RowColumn, Bytes> generateData(PpEdge key, Optional<Jaccard> val) {
    if (!val.isPresent()) {
      return Collections.emptyMap();
    }

    double jaccard = val.get().toDouble();

    Bytes node1 = Bytes.of(key.getNode1().getId());
    Bytes node2 = Bytes.of(key.getNode2().getId());
    Bytes fam = Bytes.of(rl.encode(jaccard));


    return ImmutableMap.of(new RowColumn(node1, new Column(fam, node2)), Bytes.EMPTY,
        new RowColumn(node2, new Column(fam, node1)), Bytes.EMPTY);
  }
}
