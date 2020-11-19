package org.apache.kudu.examples;

import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.commons.lang3.time.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class ScanItemTable {
    public static List<Long> readFileLines(String fileName) {
        List<Long> lines = new ArrayList<>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(fileName));
            String line = reader.readLine();
            while (line != null) {
                lines.add(Long.parseLong(line));
                line = reader.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
        return lines;
    }

    public static void scanItems(String itemIdFile,
                                 String kuduMasters,
                                 String tableName,
                                 int iteration) {
        System.out.println("item id file: " + itemIdFile);
        System.out.println("kudu masters: " + kuduMasters);
        System.out.println("kudu table: " + tableName);
        System.out.println("iteration: " + iteration);
        KuduClient client = new KuduClient.KuduClientBuilder(kuduMasters).build();
        try {
            // read all item ids
            List<Long> allItemIds = readFileLines(itemIdFile);
            if (allItemIds.isEmpty()) {
                System.out.println("No item ID to scan in " + itemIdFile);
                return;
            }
            // create the table scanner
            KuduTable kuduTable = client.openTable(tableName);
            Schema schema = kuduTable.getSchema();
            System.out.println("Table '" + tableName + "' colums: " + schema.getColumnCount());

            List<String> projectColumns = new ArrayList<>(1);
            projectColumns.add("curnt_price");
            List<KuduScanner> scanners = new ArrayList<>(allItemIds.size());
            for (Long item : allItemIds) {
                KuduPredicate predicate = KuduPredicate.newComparisonPredicate(
                        schema.getColumn("item_id"),
                        KuduPredicate.ComparisonOp.EQUAL,
                        item);
                KuduScanner scanner = client.newScannerBuilder(kuduTable)
                        .setProjectedColumnNames(projectColumns)
                        .cacheBlocks(false)
                        .addPredicate(predicate)
                        .build();
                scanners.add(scanner);
            }
            List<Double> prices = new ArrayList<Double>(allItemIds.size());
            for (int k = 0; k < scanners.size(); k++) {
                prices.add(-1.0);
            }
            // run the perf test
            StopWatch watch = new StopWatch();
            watch.start();
            for (int i = 0; i < iteration; i++) {
                int k = ThreadLocalRandom.current().nextInt(0, scanners.size());
                KuduScanner scanner = scanners.get(k);
                double res = -1;
                while (scanner.hasMoreRows()) {
                    RowResultIterator results = null;
                    try {
                        results = scanner.nextRows();
                        while (results.hasNext()) {
                            RowResult result = results.next();
                            res = result.getDouble("curnt_price");
                        }
                    } catch (KuduException ke) {
                        ke.printStackTrace();
                    }
                }
                if (res != -1) {
                    prices.set(k, res);
                }
            }
            watch.stop();
            System.out.println("Run " + iteration + " scans take " + watch.getTime() + " ms");
            System.out.println("The SQL is like \"select curnt_price from " + tableName + " where item_id = ?\"");
            for (int k = 0; k < scanners.size(); k++) {
                System.out.println("item: " + allItemIds.get(k) + " price: " + prices.get(k));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public static void main(String args[]) {
        ArgumentsParser parser = new ArgumentsParser();
        if (!parser.parseArgs(args)) {
            return;
        }

        scanItems(parser.itemsIdFile,
                  parser.kuduMasters,
                  parser.tableName,
                  parser.iteration);
    }
}
