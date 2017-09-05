package cz.net21.ttulka.thistledb.server;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Pool holder of the server connections.
 *
 * @author ttulka
 */
class ServerConnectionPool {

    private final List<ClientConnectionThread> connectionPool = new ArrayList<>();

    public void addClientConnection(ClientConnectionThread clientConnectionThread) {
        connectionPool.add(clientConnectionThread);
    }

    public boolean isConnectionPoolFree(int maxClientConnections) {
        int countOpenConnections = 0;

        Iterator<ClientConnectionThread> iterator = connectionPool.iterator();
        while (iterator.hasNext()) {
            ClientConnectionThread thread = iterator.next();
            if (thread.isLive()) {
                countOpenConnections++;
            } else {
                iterator.remove();
            }
        }
        return countOpenConnections < maxClientConnections;
    }

    public void shutdownClientConnections() {
        connectionPool.stream().forEach(ClientConnectionThread::stop);
    }
}
