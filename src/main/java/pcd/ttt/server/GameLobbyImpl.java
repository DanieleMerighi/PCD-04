package pcd.ttt.server;

import pcd.ttt.common.Game;
import pcd.ttt.common.GameException;
import pcd.ttt.common.GameLobby;
import pcd.ttt.common.GameObserver;
import pcd.ttt.common.Mark;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class GameLobbyImpl implements GameLobby {

    private static final class Session {
        final GameImpl impl;
        final PlayerProxy proxyX;
        PlayerProxy proxyO;

        Session(GameImpl impl, PlayerProxy proxyX) {
            this.impl = impl;
            this.proxyX = proxyX;
        }
    }

    private final Map<String, Session> games = new HashMap<>();
    private final ExecutorService cleanup = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService monitor = Executors.newScheduledThreadPool(2);

    @Override
    public synchronized Game createGame(String name, GameObserver observer) throws RemoteException, GameException {
        requireValidName(name);
        if (games.containsKey(name)) {
            throw new GameException("A game already exists with name: " + name);
        }
        GameImpl impl = new GameImpl(name, observer, () -> terminate(name), monitor);
        PlayerProxy proxyX = new PlayerProxy(impl, Mark.X);
        Game stub = (Game) UnicastRemoteObject.exportObject(proxyX, 0);
        games.put(name, new Session(impl, proxyX));
        impl.beginMonitoring();
        return stub;
    }

    @Override
    public Game joinGame(String name, GameObserver observer) throws RemoteException, GameException {
        requireValidName(name);
        Session session;
        synchronized (this) {
            session = games.get(name);
            if (session == null) {
                throw new GameException("No game with name: " + name);
            }
        }
        session.impl.join(observer);
        PlayerProxy proxyO = new PlayerProxy(session.impl, Mark.O);
        Game stub = (Game) UnicastRemoteObject.exportObject(proxyO, 0);
        synchronized (this) {
            if (games.get(name) != session) {
                scheduleUnexport(proxyO);
                throw new GameException("Game no longer available: " + name);
            }
            session.proxyO = proxyO;
        }
        return stub;
    }

    @Override
    public synchronized List<String> listGames() throws RemoteException {
        return games.keySet().stream().sorted().toList();
    }

    private static void requireValidName(String name) throws GameException {
        if (name == null || name.isBlank()) {
            throw new GameException("Game name must not be empty");
        }
    }

    private void terminate(String name) {
        PlayerProxy x, o;
        synchronized (this) {
            Session session = games.remove(name);
            if (session == null) {
                return;
            }
            x = session.proxyX;
            o = session.proxyO;
        }
        scheduleUnexport(x);
        if (o != null) {
            scheduleUnexport(o);
        }
    }

    private void scheduleUnexport(Remote remote) {
        cleanup.execute(() -> unexportWhenIdle(remote));
    }

    private void unexportWhenIdle(Remote remote) {
        try {
            for (int i = 0; i < 100; i++) {
                if (UnicastRemoteObject.unexportObject(remote, false)) {
                    return;
                }
                Thread.sleep(10);
            }
            UnicastRemoteObject.unexportObject(remote, true);
        } catch (Exception ignored) {
        }
    }
}
