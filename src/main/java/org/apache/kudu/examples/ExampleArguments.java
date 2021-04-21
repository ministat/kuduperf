package org.apache.kudu.examples;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class ExampleArguments {
    @Option(name = "-k", aliases = "--kuduMasters", usage = "Specify the kudu masters")
    public String kuduMasters;

    @Option(name = "-m", aliases = "--operationMode",
            usage = "Specify the operation mode: [1/3/7/15/31]. 1: create table, 3: insert data to existing table, 7: alter table, 15: scanning table, 31: delete table. Default is 1")
    public int mode = 1;

    @Option(name = "-t", aliases = "--tableName", usage = "Specify the table name, otherwise it is java_example-xxx")
    public String tableName = null;

    @Option(name = "-x", aliases = "--removeTable", usage = "Remove the table specified by --tableName")
    public boolean removeTable = false;

    @Option(name = "-p", aliases = "--principal", usage = "Specify the principal name")
    public String principalName = null;

    @Option(name = "-e", aliases = "--keytab", usage = "Specify the keytab file")
    public String keytab = null;

    @Option(name = "-b", aliases = "--useKerberos",
            usage = "use Kerberos authentication. Default is false")
    public boolean useKerberos = false;

    @Option(name = "-l", aliases = "--listTables", usage = "List all tables")
    public boolean listTable = false;

    @Option(name = "-n", aliases = "--threads", usage = "Sepcify the thread number")
    public int threads = 1;

    @Option(name = "-r", aliases = "--rows", usage = "Specify the row number")
    public int rows = 150;

    @Option(name = "-d", aliases = "--durationMs", usage = "Specify the duration milli-seconds for stress test. -1 means disable stress test")
    public int duration = -1;

    @Option(name = "-u", aliases = "--prometheus", usage = "Specify the prometheus endpoint: e.g. hms-asset00445326.stratus.lvs.ebay.com:9091")
    public String prometheus_endpoint;

    public boolean parseArgs(final String[] args) {
        final CmdLineParser parser = new CmdLineParser(this);
        if (args.length < 1) {
            parser.printUsage(System.out);
            System.exit(-1);
        }
        boolean ret = true;
        try {
            parser.parseArgument(args);
        } catch (CmdLineException ex) {
            System.out.println("Error: failed to parse command-line opts: " + ex);
            ret = false;
        }
        return ret;
    }
}
