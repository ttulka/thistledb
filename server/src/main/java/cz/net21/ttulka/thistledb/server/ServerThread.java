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

    private final Listening serverListening;
    private final Runnable onFinished;

    private final QueryProcessor queryProcessor;

    public ServerThread(@NonNull Socket socket, @NonNull DataSource dataSource,
                        Listening serverListening, Runnable onFinished) {
        super();
        this.socket = socket;
        this.serverListening = serverListening;
        this.onFinished = onFinished;

        this.queryProcessor = new QueryProcessor(dataSource);
    }

    @Override
    public void run() {
        log.debug("Serving a client request...");

        try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))
        ) {
            String input;
            while (serverListening.listening() && (input = in.readLine()) != null) {
                queryProcessor.process(input.trim(), out);
            }
        } catch (IOException e) {
            log.error("Error while serving a client socket.", e);
        } finally {
            onFinished.run();
        }
    }
}

@FunctionalInterface
interface Listening {
    boolean listening();
}