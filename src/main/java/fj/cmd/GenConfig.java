package fj.cmd;

import javax.inject.Inject;

import fj.Application;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.recipes.accumulo.export.AccumuloExporter;

public class GenConfig {

  @Inject
  static FluoConfiguration fluoConfig;

  static final String EXPORT_TABLE_NAME = "jaccardQuery";

  public static void main(String[] args) throws ConfigurationException {
    FluoConfiguration newConfig = new FluoConfiguration();

    SimpleConfiguration exportConf = new AccumuloExporter.Configuration(
        fluoConfig.getAccumuloInstance(), fluoConfig.getAccumuloZookeepers(),
        fluoConfig.getAccumuloUser(), fluoConfig.getAccumuloPassword(), EXPORT_TABLE_NAME);
    Application.addObserverConfig(newConfig, exportConf);

    newConfig.save(System.out);
  }
}
