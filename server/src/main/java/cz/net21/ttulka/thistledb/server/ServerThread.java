package cz.net21.ttulka.thistledb.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import cz.net21.ttulka.thistledb.server.db.DataSource;
import lombok.NonNull;
import lombok.extern.apachecommons.CommonsLog;

/**
 * Created by ttulka
 * <p>
 * Application thread to server a client connection.
 */
@CommonsLog
class ServerThread implements Runnable {

    private final Socket socket;

    private final Stoppable serverStopCmd;
    private final Listening serverListening;

    private final Processor processor;

    public ServerThread(@NonNull Socket socket, @NonNull DataSource dataSource, Stoppable serverStopCmd, Listening serverListening) {
        super();
        this.socket = socket;
        this.serverStopCmd = serverStopCmd;
        this.serverListening = serverListening;

        this.processor = new Processor(dataSource);
    }

    @Override
    public void run() {
        boolean stopCommandReceived = false;

        try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            String input;

            while (serverListening.listening() && (input = in.readLine()) != null) {

                if (Commands.SERVER_STOP.toString().equals(input)) {
                    stopCommandReceived = true;
                    break;
                }

                processor.process(input, out);
            }
        } catch (IOException e) {
            log.error("Error while serving a client socket.", e);
        }

        try {
            socket.close();
        } catch (IOException e) {
            log.warn("Cannot close a socket.", e);
        }

        if (stopCommandReceived) {
            serverStopCmd.stop();
        }
    }
}

@FunctionalInterface
interface Stoppable {
    void stop();
}

@FunctionalInterface
interface Listening {
    boolean listening();
}