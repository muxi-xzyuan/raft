package edu.jit.stackfarm.xzy.raft.kvstore.client;

import edu.jit.stackfarm.xzy.raft.core.node.NodeId;
import edu.jit.stackfarm.xzy.raft.core.rpc.Address;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.Map;

public class ConsoleLauncher {

    public static void main(String[] args) {
        ConsoleLauncher launcher = new ConsoleLauncher();
        launcher.execute(args);
    }

    private void execute(String[] args) {
        Options options = new Options();
        options.addOption(Option.builder("gc")
                .hasArgs()
                .argName("server-config")
                .required()
                .desc("group config, required. format: <server-config> <server-config>. " +
                        "format of server config: <node-id>,<host>,<port-service>. e.g A,localhost,8001 B,localhost,8011")
                .build());
        if (args.length == 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("raft-kvstore-client [OPTION]...", options);
            return;
        }

        CommandLineParser parser = new DefaultParser();
        Map<NodeId, Address> serverMap;
        try {
            CommandLine commandLine = parser.parse(options, args);
            serverMap = parseGroupConfig(commandLine.getOptionValues("gc"));
        } catch (ParseException | IllegalArgumentException e) {
            System.err.println(e.getMessage());
            return;
        }

        Console console = new Console(serverMap);
        console.start();
    }

    private Map<NodeId, Address> parseGroupConfig(String[] rawGroupConfig) {
        Map<NodeId, Address> serverMap = new HashMap<>();
        for (String rawServerConfig : rawGroupConfig) {
            ServerConfig serverConfig = parseServerConfig(rawServerConfig);
            serverMap.put(new NodeId(serverConfig.getNodeId()), new Address(serverConfig.getHost(), serverConfig.getPort()));
        }
        return serverMap;
    }

    private ServerConfig parseServerConfig(String rawServerConfig) {
        String[] pieces = rawServerConfig.split(",");
        if (pieces.length != 3) {
            throw new IllegalArgumentException("illegal server config [" + rawServerConfig + "]");
        }
        String nodeId = pieces[0];
        String host = pieces[1];
        int port;
        try {
            port = Integer.parseInt(pieces[2]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Illegal port [" + pieces[2] + "]");
        }
        return new ServerConfig(nodeId, host, port);
    }

    @Getter(AccessLevel.PACKAGE)
    @AllArgsConstructor(access = AccessLevel.PACKAGE)
    private static class ServerConfig {
        private final String nodeId;
        private final String host;
        private final int port;
    }
}
