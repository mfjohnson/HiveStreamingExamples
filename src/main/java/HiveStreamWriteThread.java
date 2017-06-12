import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.streaming.DelimitedInputWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.TransactionBatch;

import java.util.Date;

/**
 * Created by mjohnson on 3/3/16.
 */
public class HiveStreamWriteThread extends Thread {
    String hostname = null;
    String dbName = null;
    String tblName = null;
    StreamingConnection con = null;
    HiveConf conf = null;
    String[]  columnFields = null;
    int threadId = 0;
    int maxBatchGroups = 1000;
    int maxBatchSize = 100;   //CONTROLS delta FILE SIZE.  Higher the number, larger the delta file
    int writeRows =   10000000;

    public HiveStreamWriteThread(int ti, String h, String db, String tbl, StreamingConnection c, HiveConf hc, String[] cols) {
        hostname = h;
        dbName = db;
        tblName = tbl;
        con = c;
        conf = hc;
        columnFields = cols;
        threadId = ti+1;

    }

    public void run() {
        writeStream(con,conf);
    }

    private void writeStream(StreamingConnection connection, HiveConf hiveConf) {
        HiveEndPoint endPt = new HiveEndPoint("thrift://"+hostname+":9083", dbName, tblName, null);
        DelimitedInputWriter writer = null;

        try {
            Date startDate = new Date();
            connection = endPt.newConnection(false);
            writer = new DelimitedInputWriter(columnFields, ",", endPt);
            
            int currentBatchSize = maxBatchSize;

            TransactionBatch txnBatch = connection.fetchTransactionBatch(maxBatchGroups, writer);
            txnBatch.beginNextTransaction();

            for (int i = 0; i <writeRows; ++i) {
                writeStream(i, connection, writer, txnBatch, false);
                if (currentBatchSize < 1) {
                    System.out.println("->"+threadId+" Begining Transaction Commit:"+i+" Transaction State:"
                            +txnBatch.getCurrentTransactionState());
                    writer.flush();
                    txnBatch.commit();

                    if (txnBatch.remainingTransactions() > 0) {
                        System.out.println("->"+threadId+" ->"+i+" txnBatch transactions remaining:"
                                + txnBatch.remainingTransactions());
                        txnBatch.beginNextTransaction();
                        currentBatchSize = maxBatchSize;
                    } else {
                        System.out.println("->"+threadId+" Refereshing the transaction group count");
                        txnBatch = connection.fetchTransactionBatch(maxBatchGroups, writer);
                        txnBatch.beginNextTransaction();
                        currentBatchSize = maxBatchSize;

                    }
                }
                --currentBatchSize;
            }
            writer.flush();
            txnBatch.commit();
            txnBatch.close();


            Date endDate = new Date();
            long elapsedTime = endDate.getTime() - startDate.getTime();
            long avgMs = writeRows/elapsedTime;
            System.out.println("Elapsed time for "+writeRows+" rows was "+avgMs);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    }

    private void writeStream(int i, StreamingConnection connection, DelimitedInputWriter writer, TransactionBatch txnBatch, boolean closeEarly) {
        try {
            String outputString = Integer.valueOf(i).toString() + ",A String Value1"+ ",A String Value2"+ ",A String Value3"+ ",A String Value4"+ ",A String Value5"+ ",A String Value6"+ ",A String Value7"+ ",A String Value8"+ ",A String Value9";
            txnBatch.write(outputString.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
