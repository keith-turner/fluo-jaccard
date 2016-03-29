package fj.model.persistence;

import java.util.Optional;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoFactory;
import fj.export.JaccardExport;
import fj.model.pojos.Jaccard;
import fj.model.pojos.PNodeId;
import fj.model.pojos.PpEdge;

public class JaccardKryoFactory implements KryoFactory {

  @Override
  public Kryo create() {
    Kryo kryo = new Kryo();
    kryo.register(PpEdge.class, 9);
    kryo.register(PNodeId.class, 10);
    kryo.register(JaccardExport.class, 11);
    kryo.register(Jaccard.class, 12);
    kryo.register(Optional.class, 13);
    return kryo;
  }

}
