package org.apache.kudu.examples;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class ArgumentsParser {
    @Option(name = "-k", aliases = "--kuduMasters", usage = "Specify the kudu masters")
    public String kuduMasters;

    @Option(name = "-t", aliases = "--tableName", usage = "Specify the kudu table name")
    public String tableName;

    @Option(name = "-f", aliases = "--itemsFile", usage = "Specify the item ID list file")
    public String itemsIdFile;

    @Option(name = "-i", aliases = "--iterations", usage = "Specify the iteration count, default is 100")
    public int iteration = 100;

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
