package org.apache.gora.dynamodb.store;

import org.apache.gora.persistency.Persistent;
import org.apache.gora.store.DataStore;

public interface IDynamoDB<K, T extends Persistent> extends DataStore<K, T> {

  /**
   * Sets the handler to the main DynamoDB
   * @param DynamoDBStore handler to main DynamoDB
   */
  public abstract void setDynamoDBStoreHandler(DynamoDBStore<K, T> dynamoHandler);

}