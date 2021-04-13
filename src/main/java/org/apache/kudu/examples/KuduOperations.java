package org.apache.kudu.examples;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.List;

import static org.apache.kudu.examples.Utilities.convertExceptionMessage;

public class KuduOperations {
    public static final Double DEFAULT_DOUBLE = 12.345;
    static String createExampleTable(KuduClient client, String tableName)  throws KuduException {
        // Set up a simple schema.
        List<ColumnSchema> columns = new ArrayList<>(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
                .key(true)
                .build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).nullable(true)
                .build());
        Schema schema = new Schema(columns);

        // Set up the partition schema, which distributes rows to different tablets by hash.
        // Kudu also supports partitioning by key range. Hash and range partitioning can be combined.
        // For more information, see http://kudu.apache.org/docs/schema_design.html.
        CreateTableOptions cto = new CreateTableOptions();
        List<String> hashKeys = new ArrayList<>(1);
        hashKeys.add("key");
        int numBuckets = 8;
        cto.addHashPartitions(hashKeys, numBuckets);

        // Create the table.
        client.createTable(tableName, schema, cto);
        return "Created table " + tableName + System.lineSeparator();
    }

    static String insertRows(KuduClient client, String tableName, int numRows) throws KuduException {
        // Open the newly-created table and create a KuduSession.
        KuduTable table = client.openTable(tableName);
        KuduSession session = client.newSession();
        for (int i = 0; i < numRows; i++) {
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            row.addInt("key", i);
            // Make even-keyed row have a null 'value'.
            if (i % 2 == 0) {
                row.setNull("value");
            } else {
                row.addString("value", "value " + i);
            }
            session.apply(insert);
        }

        // Call session.close() to end the session and ensure the rows are
        // flushed and errors are returned.
        // You can also call session.flush() to do the same without ending the session.
        // When flushing in AUTO_FLUSH_BACKGROUND mode (the default mode recommended
        // for most workloads, you must check the pending errors as shown below, since
        // write operations are flushed to Kudu in background threads.
        session.close();
        StringBuffer sb = new StringBuffer();
        if (session.countPendingErrors() != 0) {
            // System.out.println("errors inserting rows");
            sb.append("errors inserting rows");
            org.apache.kudu.client.RowErrorsAndOverflowStatus roStatus = session.getPendingErrors();
            org.apache.kudu.client.RowError[] errs = roStatus.getRowErrors();
            int numErrs = Math.min(errs.length, 5);
            sb.append("there were errors inserting rows to Kudu").append(System.lineSeparator())
                    .append("the first few errors follow:").append(System.lineSeparator());
            for (int i = 0; i < numErrs; i++) {
                sb.append(errs[i]);
            }
            if (roStatus.isOverflowed()) {
                sb.append("error buffer overflowed: some errors were discarded").append(System.lineSeparator());
            }
            throw new RuntimeException("error inserting rows to Kudu");
        }
        sb.append("Inserted " + numRows + " rows").append(System.lineSeparator());
        return sb.toString();
    }

    static String scanTableAndCheckResults(KuduClient client, String tableName, int numRows) throws KuduException {
        KuduTable table = client.openTable(tableName);
        Schema schema = table.getSchema();
        // Scan with a predicate on the 'key' column, returning the 'value' and "added" columns.
        List<String> projectColumns = new ArrayList<>(2);
        projectColumns.add("key");
        projectColumns.add("value");
        projectColumns.add("added");
        int lowerBound = 0;
        KuduPredicate lowerPred = KuduPredicate.newComparisonPredicate(
                schema.getColumn("key"),
                KuduPredicate.ComparisonOp.GREATER_EQUAL,
                lowerBound);
        int upperBound = numRows / 2;
        KuduPredicate upperPred = KuduPredicate.newComparisonPredicate(
                schema.getColumn("key"),
                KuduPredicate.ComparisonOp.LESS,
                upperBound);
        KuduScanner scanner = client.newScannerBuilder(table)
                .setProjectedColumnNames(projectColumns)
                .addPredicate(lowerPred)
                .addPredicate(upperPred)
                .build();

        // Check the correct number of values and null values are returned, and
        // that the default value was set for the new column on each row.
        // Note: scanning a hash-partitioned table will not return results in primary key order.
        int resultCount = 0;
        int nullCount = 0;
        while (scanner.hasMoreRows()) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext()) {
                RowResult result = results.next();
                if (result.isNull("value")) {
                    nullCount++;
                }
                double added = result.getDouble("added");
                if (added != DEFAULT_DOUBLE) {
                    throw new RuntimeException("expected added=" + DEFAULT_DOUBLE +
                            " but got added= " + added);
                }
                resultCount++;
            }
        }
        int expectedResultCount = upperBound - lowerBound;
        if (resultCount != expectedResultCount) {
            throw new RuntimeException("scan error: expected " + expectedResultCount +
                    " results but got " + resultCount + " results");
        }
        int expectedNullCount = expectedResultCount / 2 + (expectedResultCount % 2 == 0 ? 0 : 1);
        if (nullCount != expectedNullCount) {
            throw new RuntimeException("scan error: expected " + expectedNullCount +
                    " rows with value=null but found " + nullCount);
        }
        return "Scanned some rows and checked the results" + System.lineSeparator();
    }

    public static String kuduTableTest(String tableName,
                                       KuduClient client,
                                       ExampleArguments eArgParser) {
        StringBuffer sb = new StringBuffer();
        try {
            if ((eArgParser.mode & 1 ) == 1) {
                createExampleTable(client, tableName);
                sb.append("Successfully create kudu table: ").append(tableName).append(System.lineSeparator());
            }
            int numRows = eArgParser.rows;
            if ((eArgParser.mode & 2) == 2) {
                sb.append(insertRows(client, tableName, numRows));
            }

            // Alter the table, adding a column with a default value.
            // Note: after altering the table, the table needs to be re-opened.
            if ((eArgParser.mode & 4) == 4) {
                AlterTableOptions ato = new AlterTableOptions();
                ato.addColumn("added", org.apache.kudu.Type.DOUBLE, DEFAULT_DOUBLE);
                client.alterTable(tableName, ato);
                sb.append("Altered the table").append(System.lineSeparator());
            }

            if ((eArgParser.mode & 8) == 8) {
                sb.append(scanTableAndCheckResults(client, tableName, numRows));
            }
        } catch (Exception e) {
            return convertExceptionMessage(e);
        } finally {
            try {
                if ((eArgParser.mode & 16) == 16) {
                    client.deleteTable(tableName);
                    sb.append("Deleted the table").append(System.lineSeparator());
                }
            } catch (Exception e) {
                sb.append(convertExceptionMessage(e));
            } finally {
                try {
                    client.shutdown();
                } catch (Exception e) {
                    sb.append(convertExceptionMessage(e));
                }
            }
        }
        return sb.toString();
    }
}
