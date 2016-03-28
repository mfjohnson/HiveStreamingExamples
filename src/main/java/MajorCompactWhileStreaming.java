import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.streaming.StreamingConnection;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;


/**
 * Created by mjohnson on 10/30/15.
 */
public class MajorCompactWhileStreaming {
    private Driver driver;
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    IMetaStoreClient msClient;

    private static final String TEST_DATA_DIR = HCatUtil.makePathASafeFileName(System.getProperty("java.io.tmpdir") +
            File.separator + MajorCompactWhileStreaming.class.getCanonicalName() + "-" + System.currentTimeMillis());
    private static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";

    String dbName = "default";
    String tblName = "acidTest";
    String hostname = "server2.hdp";
    int threadCount = 10;

    /* Two Columns example definitions */
    String twoColumnsCreate = "CREATE TABLE " + tblName + "( b STRING) " +
            " PARTITIONED by (a int) STORED AS ORC tblproperties(\"transactional\"=\"true\")";
    String[] twoColumns = new String[]{"a", "b"};

    /* Ten Columns example definitions */
    String tenColumnsCreate = "CREATE TABLE " + tblName + "(a INT, b1 STRING, b2 STRING,b3 STRING,b4 STRING,b5 STRING,b6 STRING,b7 STRING,b8 STRING,b9 STRING) " +
            " CLUSTERED BY(a) INTO 2 BUCKETS STORED AS ORC";
    //" CLUSTERED BY(a) INTO 2 BUCKETS STORED AS ORC tblproperties(\"transactional\"=\"true\")";
    String[] tenColumns = new String[]{"a", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9"};

    public MajorCompactWhileStreaming() {

        StreamingConnection connection = null;
        System.out.println("Preparing the environment....");

        HiveConf hiveConf = prepareEnvironment();



        System.out.println("   Creating the tables to receive the Hive Streaming...");
        setupTestTable(tblName, hiveConf, tenColumnsCreate);
        System.out.println("         Getting the end point");

        HiveStreamWriteThread[] threadList = new HiveStreamWriteThread[threadCount];
        for (int i = 0; i < threadCount; ++i) {
            threadList[i] = new HiveStreamWriteThread(i, hostname, dbName, tblName, connection, hiveConf, tenColumns);
            threadList[i].start();
        }
    }



    private void setupTestTable(String tblName, HiveConf hiveConf, String colsCreate) {
        try {
            msClient = new HiveMetaStoreClient(hiveConf);
            driver = new Driver(hiveConf);
            SessionState.start(new CliSessionState(hiveConf));

            String dropTable = "drop table if exists " + tblName, driver;


            try {
                Class.forName(driverName);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }
            Connection con = DriverManager.getConnection("jdbc:hive2://"+hostname+":10000/default", "mjohnson", "hive");

            String createTable_sql = colsCreate;
            Statement stmt = con.createStatement();

            stmt.executeUpdate(dropTable);
            stmt.executeUpdate(createTable_sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private HiveConf prepareEnvironment() {
        HiveConf hiveConf = new HiveConf(this.getClass());
        try {

            hiveConf.setVar(HiveConf.ConfVars.PREEXECHOOKS, "");
            hiveConf.setVar(HiveConf.ConfVars.POSTEXECHOOKS, "");
            hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, TEST_WAREHOUSE_DIR);
            hiveConf.setVar(HiveConf.ConfVars.HIVEINPUTFORMAT, HiveInputFormat.class.getName());
            hiveConf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");

            TxnDbUtil.setConfValues(hiveConf);
            TxnDbUtil.cleanDb();
            TxnDbUtil.prepDb();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return hiveConf;
    }


    public static void main(String[] args) {
        MajorCompactWhileStreaming mcs = new MajorCompactWhileStreaming();

    }
}
