package fj.logic;

import java.util.Set;

import com.google.common.collect.Sets;
import fj.model.persistence.SNodePersistence;
import fj.model.pojos.PsEdge;
import org.apache.fluo.recipes.core.types.TypedLoader;
import org.apache.fluo.recipes.core.types.TypedTransactionBase;

public class EdgeLoader extends TypedLoader {

  private Set<PsEdge> edges;

  public EdgeLoader(Set<PsEdge> edges) {
    this.edges = edges;
  }

  @Override
  public void load(TypedTransactionBase tx, Context context) throws Exception {
    Set<PsEdge> existingEdges = SNodePersistence.lookupEdges(tx, edges);
    SNodePersistence.addEdges(tx, Sets.difference(edges, existingEdges));
  }
}
