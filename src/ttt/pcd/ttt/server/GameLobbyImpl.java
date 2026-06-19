package pcd.ttt.server;

import pcd.ttt.common.Game;
import pcd.ttt.common.GameException;
import pcd.ttt.common.GameLobby;
import pcd.ttt.common.GameObserver;
import pcd.ttt.common.Mark;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class GameLobbyImpl implements GameLobby {

    private static final class Session {
        final GameImpl impl;
        final PlayerProxy proxyX;
        volatile PlayerProxy proxyO;

        Session(GameImpl impl, PlayerProxy proxyX) {
            this.impl = impl;
            this.proxyX = proxyX;
        }
    }

    private final Map<String, Session> games = new ConcurrentHashMap<>();
    private final ScheduledExecutorService monitor = Executors.newScheduledThreadPool(2);

    @Override
    public Game createGame(String name, GameObserver observer) throws RemoteException, GameException {
        requireValidName(name);
        GameImpl impl = new GameImpl(name, observer, () -> terminate(name), monitor);
        PlayerProxy proxyX = new PlayerProxy(impl, Mark.X);
        if (games.putIfAbsent(name, new Session(impl, proxyX)) != null) {
            throw new GameException("A game already exists with name: " + name);
        }
        Game stub = (Game) UnicastRemoteObject.exportObject(proxyX, 0);
        impl.beginMonitoring();
        return stub;
    }

    @Override
    public Game joinGame(String name, GameObserver observer) throws RemoteException, GameException {
        requireValidName(name);
        Session session = games.get(name);
        if (session == null) {
            throw new GameException("No game with name: " + name);
        }
        session.impl.join(observer);
        PlayerProxy proxyO = new PlayerProxy(session.impl, Mark.O);
        Game stub = (Game) UnicastRemoteObject.exportObject(proxyO, 0);
        Session current = games.computeIfPresent(name, (k, s) -> {
            if (s == session) {
                s.proxyO = proxyO;
            }
            return s;
        });
        if (current != session) {
            unexport(proxyO);
            throw new GameException("Game no longer available: " + name);
        }
        return stub;
    }

    @Override
    public List<String> listGames() throws RemoteException {
        return games.keySet().stream().sorted().toList();
    }

    private static void requireValidName(String name) throws GameException {
        if (name == null || name.isBlank()) {
            throw new GameException("Game name must not be empty");
        }
    }

    private void terminate(String name) {
        Session session = games.remove(name);
        if (session == null) {
            return;
        }
        unexport(session.proxyX);
        if (session.proxyO != null) {
            unexport(session.proxyO);
        }
    }

    private void unexport(Remote remote) {
        try {
            UnicastRemoteObject.unexportObject(remote, true);
        } catch (Exception ignored) {
        }
    }
}
