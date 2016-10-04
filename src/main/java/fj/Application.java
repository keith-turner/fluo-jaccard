package fj;

import fj.logic.PNodeObserver;
import fj.logic.SNodeObserver;
import fj.model.persistence.JaccardKryoFactory;
import fj.model.pojos.Jaccard;
import fj.model.pojos.PpEdge;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.recipes.accumulo.export.AccumuloExporter;
import org.apache.fluo.recipes.core.export.ExportQueue;
import org.apache.fluo.recipes.kryo.KryoSimplerSerializer;

public class Application {
  public static final String EXPORT_QUEUE_ID = "je";
  private static final int NUM_BUCKETS = 13;


  public static void addObserverConfig(FluoConfiguration fluoConfig,
      SimpleConfiguration exporterConf) {

    fluoConfig.addObserver(new ObserverSpecification(PNodeObserver.class.getName()));
    fluoConfig.addObserver(new ObserverSpecification(SNodeObserver.class.getName()));

    // setup an export queue
    ExportQueue.configure(fluoConfig,
        new ExportQueue.Options(EXPORT_QUEUE_ID, AccumuloExporter.class.getName(),
            PpEdge.class.getName(), Jaccard.class.getName(), NUM_BUCKETS)
                .setExporterConfiguration(exporterConf));

    KryoSimplerSerializer.setKryoFactory(fluoConfig, JaccardKryoFactory.class);
  }
}
