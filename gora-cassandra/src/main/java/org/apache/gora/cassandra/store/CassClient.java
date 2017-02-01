package org.apache.gora.cassandra.store;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import org.apache.gora.persistency.impl.PersistentBase;

import java.util.Map;
import java.util.Properties;
/**
 * Created by renatomarroquin on 2017-01-30.
 */
public class CassClient<K, T extends PersistentBase> {
    private Class<K> keyClass;
    private Class<T> persistentClass;
    private CassandraMapping cassandraMapping;
    private Cluster cluster;
    private Session session;

    public void initialize(Class<K> keyClass, Class<T> persistentClass, Properties properties) throws Exception {
        this.keyClass = keyClass;
        // get cassandra mapping with persistent class
        this.persistentClass = persistentClass;
        this.cassandraMapping = CassandraMappingManager.getManager().get(persistentClass);
        String username = "";
        String password = "";
        String clusterName = "";
        String contactPoint = "";

        if (properties != null) {
            username = properties.getProperty("gora.cassandrastore.username");
            password = properties.getProperty("gora.cassandrastore.password");
            clusterName = properties.getProperty("gora.cassandrastore.cluster");
            contactPoint = properties.getProperty("gora.cassandrastore.host");
        }

        Cluster.Builder clBuilder = Cluster.builder().addContactPoint(contactPoint).withClusterName(clusterName);
        if (username != null || password != null) {
            clBuilder.withCredentials(username, password);
        }
        this.cluster = clBuilder.build();
        this.session = this.cluster.connect();
    }

    public void createKeyspace(String ks, int ksRepFactor, String ksRepStrategy) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE KEYSPACE ").append(ks).append(" WITH replication = {'class':'");
        sb.append(ksRepStrategy).append("', 'replication_factor':").append(ksRepFactor).append("};");
        session.execute(sb.toString());
    }

    public CassandraMapping getMapping() {
        return this.cassandraMapping;
    }

    public String getColType(String colName) {
        String colType = "";
        try {
            colType = persistentClass.getDeclaredField(colName).getType().getName();
        } catch (java.lang.NoSuchFieldException e) {
            e.printStackTrace();
        }
        return colType;
    }

    public void addColumn(String colFamily, String colName, String colTtl, String colType) {

    }

    public void createTable(String ksName, String colFamily, Map<String, String> colsAttribs) {
        SchemaBuilder.createTable(ksName, colFamily);
//        colsAttribs.forEach((colName, colTtl) -> {
//            String colType = getColType(colName);
//        });

    }
}
