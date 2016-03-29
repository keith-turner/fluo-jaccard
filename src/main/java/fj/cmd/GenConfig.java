package fj.cmd;

import javax.inject.Inject;

import fj.Application;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.recipes.accumulo.export.TableInfo;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class GenConfig {

  @Inject
  static FluoConfiguration fluoConfig;

  static final String EXPORT_TABLE_NAME = "jaccardQuery";

  public static void main(String[] args) throws ConfigurationException {
    FluoConfiguration newConfig = new FluoConfiguration();

    TableInfo exportTable = new TableInfo(fluoConfig.getAccumuloInstance(), fluoConfig.getAccumuloZookeepers(), fluoConfig.getAccumuloUser(), fluoConfig.getAccumuloPassword(), EXPORT_TABLE_NAME);
    Application.addObserverConfig(newConfig, exportTable);

    PropertiesConfiguration propsConfig = new PropertiesConfiguration();
    propsConfig.setDelimiterParsingDisabled(true);
    propsConfig.copy(newConfig);
    propsConfig.save(System.out);
  }
}
