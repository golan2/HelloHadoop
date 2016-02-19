package golan.hello.hadoop.utils;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.io.File;

public class SSHConnection {

    private static Session establish(CmdOpts opts) throws JSchException {
        if (opts.getSshKeyFile() != null) {
            return establishWithKey(
                    opts.getSshHost(),
                    opts.getSshPort(),
                    opts.getSshUser(),
                    opts.getSshKeyFile()
            );
        } else {
            return establishWithPassword(
                    opts.getSshHost(),
                    opts.getSshPort(),
                    opts.getSshUser(),
                    opts.getSshPassword()
            );
        }
    }

    private static Session establishWithPassword(String sshHost, int sshPort, String user, String password) throws JSchException {
        Session session;
        JSch jsch = new JSch();
        try {
            session = jsch.getSession(user, sshHost, sshPort);
            session.setPassword(password);
        }
        catch (JSchException e) {
            System.out.println("SSH connection attempt to host: " + sshHost + ":" + sshPort + " failed");
            throw e;
        }
        return connect(session, sshHost, sshPort);
    }

    private static Session establishWithKey(String sshHost, int sshPort, String user, String keyFilePath) throws JSchException {
        File keyFile = new File(keyFilePath);
        if (!keyFile.exists()) {
            String errorMsg = "Could not find SSH public key file in path: " + keyFilePath;
            System.out.println(errorMsg);
            throw new JSchException(errorMsg);
        }
        Session session;
        JSch jsch = new JSch();
        try {
            jsch.addIdentity(keyFile.getAbsolutePath());
            session = jsch.getSession(user, sshHost, sshPort);
        }
        catch (JSchException e) {
            System.out.println("SSH connection attempt to host: " + sshHost + ":" + sshPort + " failed");
            throw e;
        }
        return connect(session, sshHost, sshPort);
    }

    private static Session connect(Session session, String sshHost, int sshPort) throws JSchException {
        session.setConfig("StrictHostKeyChecking", "no");
        session.setConfig("ConnectionAttempts", "3");
        System.out.println("Establishing SSH Connection to host: " + sshHost + ":" + sshPort + "...");
        try {
            session.connect();
        }
        catch (JSchException e) {
            System.out.println("SSH connection attempt to host: " + sshHost + ":" + sshPort + " failed");
            throw e;
        }
        System.out.println("Connected to: " + sshHost + ":" + sshPort + " via SSH");
        return session;
    }

    public static Session openSshSession(CmdOpts opts) throws JSchException {
        Session sshSession = establish(opts);
        sshSession.setPortForwardingL(9000, "localhost", 9000);
        sshSession.setPortForwardingL(50090 , "localhost", 50090);
        sshSession.setPortForwardingL(50091 , "localhost", 50091);
        sshSession.setPortForwardingL(50010 , "localhost", 50010);
        sshSession.setPortForwardingL(50075 , "localhost", 50075);
        sshSession.setPortForwardingL(50020 , "localhost", 50020);
        sshSession.setPortForwardingL(50070 , "localhost", 50070);
        sshSession.setPortForwardingL(50475 , "localhost", 50475);
        sshSession.setPortForwardingL(50470 , "localhost", 50470);
        sshSession.setPortForwardingL(50100 , "localhost", 50100);
        sshSession.setPortForwardingL(50105 , "localhost", 50105);
        sshSession.setPortForwardingL(8485  , "localhost", 8485 );
        sshSession.setPortForwardingL(8480  , "localhost", 8480 );
        sshSession.setPortForwardingL(8481  , "localhost", 8481 );
        return sshSession;
    }
}

