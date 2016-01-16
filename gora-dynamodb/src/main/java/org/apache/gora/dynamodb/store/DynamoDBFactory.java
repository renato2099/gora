package org.apache.gora.dynamodb.store;

import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.PersistentBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoDBFactory {

  /** Helper to write useful information into the logs. */
  public static final Logger LOG = LoggerFactory
      .getLogger(DynamoDBFactory.class);

  public static <K, T extends Persistent> IDynamoDB<K, T> buildDynamoDBStore(
      DynamoDBUtils.DynamoDBType serType) {
    final IDynamoDB<K, T> ds;
    switch (serType) {
      case DYNAMO:
        ds = new DynamoDBNativeStore<K, T>();
        LOG.debug("Using DynamoDB based serialization mode.");
        break;
      case AVRO:
        ds = (IDynamoDB<K, T>) new DynamoDBAvroStore<K, PersistentBase>();
        LOG.debug("Using Avro based serialization mode.");
        break;
      default:
        throw new IllegalStateException("Serialization mode not supported.");
    }
    return ds;
  }
}
