package fj.model.persistence;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoFactory;
import fj.export.JaccardExporter;
import fj.model.pojos.Jaccard;
import fj.model.pojos.PNodeId;
import fj.model.pojos.PpEdge;

public class JaccardKryoFactory implements KryoFactory {

  @Override
  public Kryo create() {
    Kryo kryo = new Kryo();
    kryo.register(PpEdge.class, 9);
    kryo.register(PNodeId.class, 10);
    kryo.register(JaccardExporter.class, 11);
    kryo.register(Jaccard.class, 12);
    return kryo;
  }

}
