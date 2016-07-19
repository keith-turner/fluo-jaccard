package fj.cmd;

import javax.inject.Inject;

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

public class CreateExport {

  @Inject
  static FluoConfiguration fluoConfig;


  static Connector getConnector(FluoConfiguration fluoConfig) throws Exception {
    ZooKeeperInstance zki = new ZooKeeperInstance(new ClientConfiguration().withInstance(fluoConfig.getAccumuloInstance()).withZkHosts(fluoConfig.getAccumuloZookeepers()));
    Connector conn = zki.getConnector(fluoConfig.getAccumuloUser(), new PasswordToken(fluoConfig.getAccumuloPassword()));
    return conn;
  }

  public static void main(String[] args) throws Exception {

    Connector conn = getConnector(fluoConfig);

    try{
      conn.tableOperations().create(GenConfig.EXPORT_TABLE_NAME);
    } catch (TableExistsException tee) {
      conn.tableOperations().delete(GenConfig.EXPORT_TABLE_NAME);
      conn.tableOperations().create(GenConfig.EXPORT_TABLE_NAME);
    }
  }
}
