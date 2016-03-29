package fj;

import fj.logic.PNodeObserver;
import fj.logic.SNodeObserver;
import fj.model.persistence.JaccardKryoFactory;
import fj.model.pojos.PpEdge;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.recipes.accumulo.export.AccumuloExport;
import io.fluo.recipes.accumulo.export.AccumuloExporter;
import io.fluo.recipes.accumulo.export.TableInfo;
import io.fluo.recipes.export.ExportQueue;
import io.fluo.recipes.kryo.KryoSimplerSerializer;

public class Application {
  public static final String EXPORT_QUEUE_ID = "je";
  private static final int NUM_BUCKETS = 13;


  public static void addObserverConfig(FluoConfiguration fluoConfig, TableInfo exportTable) {

    fluoConfig.addObserver(new ObserverConfiguration(PNodeObserver.class.getName()));
    fluoConfig.addObserver(new ObserverConfiguration(SNodeObserver.class.getName()));

    //setup an export queue
    ExportQueue.configure(fluoConfig, new ExportQueue.Options(EXPORT_QUEUE_ID,
        AccumuloExporter.class.getName(), PpEdge.class.getName(), AccumuloExport.class.getName(),
        NUM_BUCKETS));

    //tie export queue to an accumulo table for export
    AccumuloExporter.setExportTableInfo(fluoConfig, EXPORT_QUEUE_ID, exportTable);

    KryoSimplerSerializer.setKryoFactory(fluoConfig, JaccardKryoFactory.class);
  }
}
