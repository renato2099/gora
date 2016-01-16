package org.apache.gora.dynamodb.store;

import static org.apache.gora.dynamodb.store.DynamoDBUtils.WS_PROVIDER;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.gora.dynamodb.query.DynamoDBKey;
import org.apache.gora.dynamodb.query.DynamoDBQuery;
import org.apache.gora.dynamodb.query.DynamoDBResult;
import org.apache.gora.persistency.BeanFactory;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.query.PartitionQuery;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.ws.impl.WSDataStoreBase;
import org.apache.gora.util.GoraException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;

public class DynamoDBNativeStore<K, T extends Persistent> extends
    WSDataStoreBase<K, T> implements IDynamoDB<K, T> {

  /** Method's names for getting range and hash keys. */
  private static final String GET_RANGE_KEY_METHOD = "getRangeKey";
  private static final String GET_HASH_KEY_METHOD = "getHashKey";

  /** Logger for {@link DynamoDBNativeStore}. */
  public static final Logger LOG = LoggerFactory
      .getLogger(DynamoDBNativeStore.class);

  /** Handler to {@link DynamoDBStore} so common methods can be accessed. */
  private DynamoDBStore<K, T> dynamoDBStoreHandler;

  /**
   * Deletes items using a specific query
   * 
   * @throws IOException
   */
  @Override
  @SuppressWarnings("unchecked")
  public long deleteByQuery(Query<K, T> query) {
    // TODO verify whether or not we are deleting a whole row
    // String[] fields = getFieldsToQuery(query.getFields());
    // find whether all fields are queried, which means that complete
    // rows will be deleted
    // boolean isAllFields = Arrays.equals(fields
    // , getBeanFactory().getCachedPersistent().getFields());
    Result<K, T> result = execute(query);
    ArrayList<T> deletes = new ArrayList<T>();
    try {
      while (result.next()) {
        T resultObj = result.get();
        deletes.add(resultObj);

        @SuppressWarnings("rawtypes")
        DynamoDBKey dKey = new DynamoDBKey();

        dKey.setHashKey(getHashFromObj(resultObj));

        dKey.setRangeKey(getRangeKeyFromObj(resultObj));
        delete((K) dKey);
      }
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return deletes.size();
  }

  /**
   * Executes a query after building a DynamoDB specific query based on the
   * received one
   */
  @Override
  public Result<K, T> execute(Query<K, T> query) {
    DynamoDBQuery<K, T> dynamoDBQuery = buildDynamoDBQuery(query);
    DynamoDBMapper mapper = new DynamoDBMapper(
        dynamoDBStoreHandler.getDynamoDbClient());
    List<T> objList = null;
    if (DynamoDBQuery.getType().equals(DynamoDBQuery.RANGE_QUERY))
      objList = mapper.scan(persistentClass,
          (DynamoDBScanExpression) dynamoDBQuery.getQueryExpression());
    if (DynamoDBQuery.getType().equals(DynamoDBQuery.SCAN_QUERY))
      objList = mapper.scan(persistentClass,
          (DynamoDBScanExpression) dynamoDBQuery.getQueryExpression());
    return new DynamoDBResult<K, T>(this, query, objList);
  }

  @Override
  public T get(K key, String[] fields) {
    /*
     * DynamoDBQuery<K,T> query = new DynamoDBQuery<K,T>();
     * query.setDataStore(this); //query.setKeyRange(key, key);
     * //query.setFields(fields); //query.setLimit(1); Result<K,T> result =
     * execute(query); boolean hasResult = result.next(); return hasResult ?
     * result.get() : null;
     */
    return null;
  }

  @Override
  /**
   * Gets the object with the specific key
   * @throws IOException
   */
  public T get(K key) {
    T object = null;
    try {
      Object rangeKey;
      rangeKey = getRangeKeyFromKey(key);
      Object hashKey = getHashFromKey(key);
      if (hashKey != null) {
        DynamoDBMapper mapper = new DynamoDBMapper(
            dynamoDBStoreHandler.getDynamoDbClient());
        if (rangeKey != null)
          object = mapper.load(persistentClass, hashKey, rangeKey);
        else
          object = mapper.load(persistentClass, hashKey);
      } else
        throw new GoraException("Error while retrieving keys from object: "
            + key.toString());
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    } catch (GoraException ge) {
      LOG.error(ge.getMessage());
      LOG.error(ge.getStackTrace().toString());
    }
    return object;
  }

  /**
   * Creates a new DynamoDBQuery
   */
  public Query<K, T> newQuery() {
    Query<K, T> query = new DynamoDBQuery<K, T>(this);
    // query.setFields(getFieldsToQuery(null));
    return query;
  }

  /**
   * Returns a new instance of the key object.
   * 
   * @throws IOException
   */
  @Override
  public K newKey() {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * Returns a new persistent object
   * 
   * @throws IOException
   */
  @Override
  public T newPersistent() {
    T obj = null;
    try {
      obj = persistentClass.newInstance();
    } catch (InstantiationException e) {
      LOG.error("Error instantiating " + persistentClass.getCanonicalName());
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      LOG.error("Error instantiating " + persistentClass.getCanonicalName());
      e.printStackTrace();
    }
    return obj;
  }

  /**
   * Puts an object identified by a key
   * 
   * @throws IOException
   */
  @Override
  public void put(K key, T obj) {
    try {
      Object hashKey = getHashKey(key, obj);
      Object rangeKey = getRangeKey(key, obj);
      if (hashKey != null) {
        DynamoDBMapper mapper = new DynamoDBMapper(
            dynamoDBStoreHandler.getDynamoDbClient());
        if (rangeKey != null) {
          mapper.load(persistentClass, hashKey, rangeKey);
        } else {
          mapper.load(persistentClass, hashKey);
        }
        mapper.save(obj);
      } else
        throw new GoraException("No HashKey found in Key nor in Object.");
    } catch (NullPointerException npe) {
      LOG.error("Error while putting an item. " + npe.toString());
      npe.printStackTrace();
    } catch (Exception e) {
      LOG.error("Error while putting an item. " + obj.toString());
      e.printStackTrace();
    }
  }

  /**
   * Deletes the object using key
   * 
   * @return true for a successful process
   * @throws IOException
   */
  @Override
  public boolean delete(K key) {
    try {
      T object = null;
      Object rangeKey = null, hashKey = null;
      DynamoDBMapper mapper = new DynamoDBMapper(
          dynamoDBStoreHandler.getDynamoDbClient());
      for (Method met : key.getClass().getDeclaredMethods()) {
        if (met.getName().equals(GET_RANGE_KEY_METHOD)) {
          Object[] params = null;
          rangeKey = met.invoke(key, params);
          break;
        }
      }
      for (Method met : key.getClass().getDeclaredMethods()) {
        if (met.getName().equals(GET_HASH_KEY_METHOD)) {
          Object[] params = null;
          hashKey = met.invoke(key, params);
          break;
        }
      }
      if (hashKey == null)
        object = (T) mapper.load(persistentClass, key);
      if (rangeKey == null)
        object = (T) mapper.load(persistentClass, hashKey);
      else
        object = (T) mapper.load(persistentClass, hashKey, rangeKey);

      if (object == null)
        return false;

      // setting key for dynamodbMapper
      mapper.delete(object);
      return true;
    } catch (Exception e) {
      LOG.error("Error while deleting value with key " + key.toString());
      LOG.error(e.getMessage());
      return false;
    }
  }

  /**
   * Initialize the data store by reading the credentials, setting the cloud
   * provider, setting the client's properties up, setting the end point and
   * reading the mapping file
   */
  public void initialize(Class<K> keyClass, Class<T> pPersistentClass,
      Properties properties) {
    super.initialize(keyClass, pPersistentClass, properties);
    setWsProvider(WS_PROVIDER);
    if (autoCreateSchema) {
      createSchema();
    }
  }

  /**
   * Builds a DynamoDB query from a generic Query object
   * 
   * @param query
   *          Generic query object
   * @return DynamoDBQuery
   */
  private DynamoDBQuery<K, T> buildDynamoDBQuery(Query<K, T> query) {
    if (getSchemaName() == null)
      throw new IllegalStateException("There is not a preferred schema set.");

    DynamoDBQuery<K, T> dynamoDBQuery = new DynamoDBQuery<K, T>();
    dynamoDBQuery.setKeySchema(dynamoDBStoreHandler.getDynamoDbMapping()
        .getKeySchema(getSchemaName()));
    dynamoDBQuery.setKeyItems(dynamoDBStoreHandler.getDynamoDbMapping().getItems(getSchemaName()));
    dynamoDBQuery.setQuery(query);
    dynamoDBQuery.setConsistencyReadLevel(dynamoDBStoreHandler
        .getConsistencyReads());
    dynamoDBQuery.buildExpression();

    return dynamoDBQuery;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  @Override
  public void flush() {
    LOG.warn("DynamoDBNativeStore puts and gets directly into the datastore");
  }

  @Override
  public BeanFactory<K, T> getBeanFactory() {
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
  public void setBeanFactory(BeanFactory<K, T> arg0) {
    // TODO Auto-generated method stub

  }

  @Override
  public void createSchema() {
    LOG.info("Creating Native DynamoDB Schemas.");
    if (dynamoDBStoreHandler.getDynamoDbMapping().getTables().isEmpty()) {
      throw new IllegalStateException("There are not tables defined.");
    }
    if (dynamoDBStoreHandler.getPreferredSchema() == null) {
      LOG.debug("Creating schemas.");
      // read the mapping object
      for (String tableName : dynamoDBStoreHandler.getDynamoDbMapping()
          .getTables().keySet())
        DynamoDBUtils.executeCreateTableRequest(
            dynamoDBStoreHandler.getDynamoDbClient(), tableName,
            dynamoDBStoreHandler.getTableKeySchema(tableName),
            dynamoDBStoreHandler.getTableAttributes(tableName),
            dynamoDBStoreHandler.getTableProvisionedThroughput(tableName));
      LOG.debug("tables created successfully.");
    } else {
      String tableName = dynamoDBStoreHandler.getPreferredSchema();
      LOG.debug("Creating schema " + tableName);
      DynamoDBUtils.executeCreateTableRequest(
          dynamoDBStoreHandler.getDynamoDbClient(), tableName,
          dynamoDBStoreHandler.getTableKeySchema(tableName),
          dynamoDBStoreHandler.getTableAttributes(tableName),
          dynamoDBStoreHandler.getTableProvisionedThroughput(tableName));
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.gora.dynamodb.store.IDynamoDB#setDynamoDBStoreHandler(org.apache
   * .gora.dynamodb.store.DynamoDBStore)
   */
  @Override
  public void setDynamoDBStoreHandler(DynamoDBStore<K, T> dynamoHandler) {
    this.dynamoDBStoreHandler = dynamoHandler;
  }

  @Override
  public void deleteSchema() {
    // TODO Auto-generated method stub

  }

  @Override
  public String getSchemaName() {
    return this.dynamoDBStoreHandler.getSchemaName();
  }

  @Override
  public boolean schemaExists() {
    return this.dynamoDBStoreHandler.schemaExists();
  }

  private Object getHashKey(K key, T obj) throws IllegalArgumentException,
      IllegalAccessException, InvocationTargetException {
    // try to get the hashKey from 'key'
    Object hashKey = getHashFromKey(key);
    // if the key does not have these attributes then try to get them from the
    // object
    if (hashKey == null)
      hashKey = getHashFromObj(obj);
    // if no key has been found, then we try with the key
    if (hashKey == null)
      hashKey = key;
    return hashKey;
  }

  /**
   * Gets a hash key from a key of type K
   * 
   * @param obj
   *          Object from which we will get a hash key
   * @return
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   */
  private Object getHashFromKey(K obj) throws IllegalArgumentException,
      IllegalAccessException, InvocationTargetException {
    Object hashKey = null;
    // check if it is a DynamoDBKey
    if (obj instanceof DynamoDBKey) {
      hashKey = ((DynamoDBKey<?, ?>) obj).getHashKey();
    } else {
      // maybe the class has the method defined
      for (Method met : obj.getClass().getDeclaredMethods()) {
        if (met.getName().equals(GET_HASH_KEY_METHOD)) {
          Object[] params = null;
          hashKey = met.invoke(obj, params);
          break;
        }
      }
    }
    return hashKey;
  }

  /**
   * Gets a hash key from an object of type T
   * 
   * @param obj
   *          Object from which we will get a hash key
   * @return
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   */
  private Object getHashFromObj(T obj) throws IllegalArgumentException,
      IllegalAccessException, InvocationTargetException {
    Object hashKey = null;
    // check if it is a DynamoDBKey
    if (obj instanceof DynamoDBKey) {
      hashKey = ((DynamoDBKey<?, ?>) obj).getHashKey();
    } else {
      // maybe the class has the method defined
      for (Method met : obj.getClass().getDeclaredMethods()) {
        if (met.getName().equals(GET_HASH_KEY_METHOD)) {
          Object[] params = null;
          hashKey = met.invoke(obj, params);
          break;
        }
      }
    }
    return hashKey;
  }

  private Object getRangeKey(K key, T obj) throws IllegalArgumentException,
      IllegalAccessException, InvocationTargetException {
    Object rangeKey = getRangeKeyFromKey(key);
    if (rangeKey == null)
      rangeKey = getRangeKeyFromObj(obj);
    return rangeKey;
  }

  /**
   * Gets a range key from a key obj. This verifies if it is using a
   * {@link DynamoDBKey}
   * 
   * @param obj
   *          Object from which a range key will be extracted
   * @return
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   */
  private Object getRangeKeyFromKey(K obj) throws IllegalArgumentException,
      IllegalAccessException, InvocationTargetException {
    Object rangeKey = null;
    // check if it is a DynamoDBKey
    if (obj instanceof DynamoDBKey) {
      rangeKey = ((DynamoDBKey<?, ?>) obj).getRangeKey();
    } else {
      // maybe the class has the method defined
      for (Method met : obj.getClass().getDeclaredMethods()) {
        if (met.getName().equals(GET_RANGE_KEY_METHOD)) {
          Object[] params = null;
          rangeKey = met.invoke(obj, params);
          break;
        }
      }
    }
    return rangeKey;
  }

  /**
   * Gets a range key from an object T
   * 
   * @param obj
   *          Object from which a range key will be extracted
   * @return
   * @throws IllegalArgumentException
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   */
  private Object getRangeKeyFromObj(T obj) throws IllegalArgumentException,
      IllegalAccessException, InvocationTargetException {
    Object rangeKey = null;
    // check if it is a DynamoDBKey
    if (obj instanceof DynamoDBKey) {
      rangeKey = ((DynamoDBKey<?, ?>) obj).getRangeKey();
    } else {
      // maybe the class has the method defined
      for (Method met : obj.getClass().getDeclaredMethods()) {
        if (met.getName().equals(GET_RANGE_KEY_METHOD)) {
          Object[] params = null;
          rangeKey = met.invoke(obj, params);
          break;
        }
      }
    }
    return rangeKey;
  }

}
