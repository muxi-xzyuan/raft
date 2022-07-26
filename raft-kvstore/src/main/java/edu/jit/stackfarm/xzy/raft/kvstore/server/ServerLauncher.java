package edu.jit.stackfarm.xzy.raft.kvstore.server;

import edu.jit.stackfarm.xzy.raft.core.node.Node;
import edu.jit.stackfarm.xzy.raft.core.node.NodeBuilder;
import edu.jit.stackfarm.xzy.raft.core.node.NodeEndpoint;
import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
// TODO load config from file
public class ServerLauncher {

    private static final String MODE_STANDALONE = "standalone";
    private static final String MODE_QUIET = "quiet";
    private static final String MODE_GROUP_MEMBER = "standard";

    public static void main(String[] args) throws Exception {
        ServerLauncher launcher = new ServerLauncher();
        launcher.execute(args);
    }

    public void execute(String[] args) throws Exception {
        Options options = new Options();
        //模式
        options.addOption(Option.builder("m")
                .hasArg()
                .argName("mode")
                .desc("start mode, available: standalone, quiet, standard.default is standalone")
                .build());
        //节点ID
        options.addOption(Option.builder("i")
                .longOpt("id")
                .hasArg()
                .argName("node-id")
                .required()
                .desc("node id, required. must be unique in group. if starts with mode standard, please ensure id in group config")
                .build());
        //主机名
        options.addOption(Option.builder("h")
                .hasArg()
                .argName("host")
                .desc("host, required when starts with standalone or quiet mode")
                .build());
        //Raft服务端口
        options.addOption(Option.builder("p1")
                .longOpt("port-raft-node")
                .hasArg()
                .argName("port")
                .type(Number.class)
                .desc("port of raft node, required when starts with standalone or quiet mode")
                .build());
        //KV服务端口
        options.addOption(Option.builder("p2")
                .longOpt("port-service")
                .hasArg()
                .argName("port")
                .type(Number.class)
                .required()
                .desc("port of service, required")
                .build());
        //日志目录
        options.addOption(Option.builder("d")
                .hasArg()
//                .required()
                .argName("data-dir")
                .desc("data directory optional. must be present")
                .build());
        //集群配置
        options.addOption(Option.builder("gc")
                .hasArgs()
                .argName("node-endpoint")
                .desc("group config, required when starts with standard mode. format: <node-endpoint> <node-endpoint>..., " +
                        "format of node-endpoint: <node-id>,<host>,<port-raft-node>, eg: A,localhost,8000 B,localhost,8010")
                .build());
        //如果main方法的参数长度为0，则输出帮助
        if (args.length == 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("raft-kvstore [OPTION]...", options);
            return;
        }

        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmdLine = parser.parse(options, args);
            String mode = cmdLine.getOptionValue('m', MODE_STANDALONE);
            switch (mode) {
                case MODE_QUIET:
                    startAsStandaloneOrQuiet(cmdLine, true);
                    break;
                case MODE_STANDALONE:
                    startAsStandaloneOrQuiet(cmdLine, false);
                    break;
                case MODE_GROUP_MEMBER:
                    startAsStandardMode(cmdLine);
                    break;
                default:
                    throw new IllegalArgumentException("illegal mode[" + mode + "]");
            }
        } catch (ParseException | IllegalArgumentException exception) {
            System.err.println(exception.getMessage());
        }
    }

    private void startAsStandaloneOrQuiet(CommandLine cmdLine, boolean quiet) throws Exception {
        if (!cmdLine.hasOption("p1") || !cmdLine.hasOption("p2")) {
            throw new IllegalArgumentException("port-raft-node or port-service required");
        }

        String id = cmdLine.getOptionValue('i');
        String host = cmdLine.getOptionValue('h', "localhost");
        int portRaftServer = ((Long) cmdLine.getParsedOptionValue("p1")).intValue();
        int portService = ((Long) cmdLine.getParsedOptionValue("p2")).intValue();

        NodeEndpoint nodeEndpoint = new NodeEndpoint(id, host, portRaftServer);
        Node node = new NodeBuilder(nodeEndpoint)
                .setQuiet(quiet)
                .setDataDir(cmdLine.getOptionValue('d'))
                .build();
        Server server = new Server(node, portService);
        log.info("Start with {} mode, id {}, host {}, raft node port {}, service port {}",
                (quiet ? "quiet" : "standalone"), id, host, portRaftServer, portService);
        startServer(server);
    }

    private void startAsStandardMode(CommandLine cmdLine) throws Exception {
        if (!cmdLine.hasOption("gc")) {
            throw new IllegalArgumentException("group-config required");
        }

        String[] rawGroupConfig = cmdLine.getOptionValues("gc");
        String rawNodeId = cmdLine.getOptionValue('i');
        int portService = ((Long) cmdLine.getParsedOptionValue("p2")).intValue();

        Set<NodeEndpoint> nodeEndpoints = Stream.of(rawGroupConfig)
                .map(this::parseNodeEndpoint)
                .collect(Collectors.toSet());

        Node node = new NodeBuilder(nodeEndpoints, new NodeId(rawNodeId))
                .setDataDir(cmdLine.getOptionValue('d'))
                .build();
        Server server = new Server(node, portService);
        log.info("start as standard mode, group config {}, id {}, service port {}", nodeEndpoints, rawNodeId, portService);
        startServer(server);
    }

    private NodeEndpoint parseNodeEndpoint(String rawNodeEndpoint) {
        String[] pieces = rawNodeEndpoint.split(",");
        if (pieces.length != 3) {
            throw new IllegalArgumentException("Illegal node endpoint [" + rawNodeEndpoint + "]");
        }
        String nodeId = pieces[0];
        String host = pieces[1];
        int port;
        try {
            port = Integer.parseInt(pieces[2]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Illegal port in node endpoint [" + rawNodeEndpoint + "]");
        }
        return new NodeEndpoint(nodeId, host, port);
    }

    private void startServer(Server server) {
        try {
            server.start();
        } catch (Exception e) {
            log.error("Application failed to start", e);
            server.stop();
        }
    }

}
