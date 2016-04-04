package golan.hello.hadoop.utils;

import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by golaniz on 17/02/2016.
 */
public class CmdOpts {
    public static final String DEF_SSH_HOST     = "myd-vm22661.hpswlabs.adapps.hp.com";
    public static final int    DEF_SSH_PORT     = 22;
    public static final String DEF_SSH_KEY_FILE = null;
    public static final String DEF_SSH_USER     = "hadoop";
    public static final String DEF_SSH_PASSWORD = "hadoop";

    private final String operation;
    private final String path;
    private final String sshHost;
    private final int    sshPort;
    private final String sshKeyFile;
    private final String sshUser;
    private final String sshPassword;

    private final Map<String, String> arguments;

    @SuppressWarnings("AccessStaticViaInstance")
    public CmdOpts(String[] args, Map<String, String> params, Set<String> mandatory) throws ParseException {
        Options o = new Options();
        o.addOption(OptionBuilder.hasArgs(1).isRequired(true).create("operation"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(true).create("path"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("sshHost"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("sshPort"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("sshKeyFile"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("sshUser"));
        o.addOption(OptionBuilder.hasArgs(1).isRequired(false).create("sshPassword"));
        CommandLineParser parser = new BasicParser();
        CommandLine line = parser.parse(o, args);

        this.operation = line.getOptionValue("operation", null);
        this.path = line.getOptionValue("path", null);
        this.sshPort = Integer.parseInt(line.getOptionValue("sshPort", String.valueOf(DEF_SSH_PORT)));
        this.sshHost = line.getOptionValue("sshHost", DEF_SSH_HOST);
        this.sshKeyFile = line.getOptionValue("sshKeyFile", DEF_SSH_KEY_FILE);
        this.sshUser = line.getOptionValue("sshUser", DEF_SSH_USER);
        this.sshPassword = line.getOptionValue("sshPassword", DEF_SSH_PASSWORD);

        arguments = new HashMap<>(params.size());
        for (String key : params.keySet()) {
            String value = line.getOptionValue(key, params.get(key));
            arguments.put(key,value);
        }
    }


    public String getOperation() { return operation; }

    public String getPath() { return path; }

    public String getSshHost() { return sshHost; }

    public int getSshPort() { return sshPort; }

    public String getSshKeyFile() { return sshKeyFile; }

    public String getSshUser() { return sshUser; }

    public String getSshPassword() { return sshPassword; }

    public String get(String name) {
        return arguments.get(name);
    }
}
