package org.apache.gora.dynamodb.store;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.gora.persistency.Persistent;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.impl.DataStoreBase;

public class DynamoDBAvroStore<K, T extends PersistentBase> extends
    DataStoreBase<K, T> implements IDynamoDB<K, T> {

  /**
   * The values are Avro fields pending to be stored.
   *
   * We want to iterate over the keys in insertion order. We don't want to lock
   * the entire collection before iterating over the keys, since in the meantime
   * other threads are adding entries to the map.
   */
  private Map<K, T> buffer = Collections
      .synchronizedMap(new LinkedHashMap<K, T>());

  private DynamoDBStore<K, ? extends Persistent> dynamoDBStoreHandler;

  /**
   * Sets the handler to the main DynamoDB
   * 
   * @param DynamoDBStore
   *          handler to main DynamoDB
   */
  @Override
  public void setDynamoDBStoreHandler(DynamoDBStore<K, T> dynamoHandler) {
    this.dynamoDBStoreHandler = dynamoHandler;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  @Override
  public void createSchema() {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean delete(K arg0) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public long deleteByQuery(Query<K, T> arg0) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void deleteSchema() {
    // TODO Auto-generated method stub

  }

  @Override
  public Result<K, T> execute(Query<K, T> arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void flush() {
    // TODO Auto-generated method stub

  }

  @Override
  public T get(K arg0, String[] arg1) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<PartitionQuery<K, T>> getPartitions(Query<K, T> arg0)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getSchemaName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Query<K, T> newQuery() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void put(K key, T value) {
    buffer.put(key, value);
  }

  @Override
  public boolean schemaExists() {
    // TODO Auto-generated method stub
    return false;
  }
}
