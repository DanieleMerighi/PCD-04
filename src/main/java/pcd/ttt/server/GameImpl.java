package pcd.ttt.server;

import pcd.ttt.common.*;

import java.rmi.RemoteException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

class GameImpl {

    private static final long HEARTBEAT_SECONDS = 3;
    private static final int MAX_MISSED_PINGS = 3;

    private static final class Player {
        final GameObserver observer;
        final ExecutorService notifier = Executors.newSingleThreadExecutor();
        int missed;

        Player(GameObserver observer) {
            this.observer = observer;
        }
    }

    private final String name;
    private final Runnable onTerminated;
    private final ScheduledExecutorService monitor;
    private ScheduledFuture<?> monitorTask;

    private final Player playerX;

    private Board board;
    private Mark turn;
    private GameStatus status;
    private Player playerO;

    GameImpl(String name, GameObserver creator, Runnable onTerminated, ScheduledExecutorService monitor) {
        this.name = name;
        this.onTerminated = onTerminated;
        this.monitor = monitor;
        this.board = new Board();
        this.turn = Mark.X;
        this.status = GameStatus.WAITING_FOR_OPPONENT;
        this.playerX = new Player(creator);
    }

    void beginMonitoring() {
        monitorTask = monitor.scheduleWithFixedDelay(this::checkAlive,
                HEARTBEAT_SECONDS, HEARTBEAT_SECONDS, TimeUnit.SECONDS);
    }

    void makeMove(Mark player, Move move) throws GameException {
        boolean terminal;
        synchronized (this) {
            if (status != GameStatus.IN_PROGRESS) {
                throw new GameException("The game is not in progress");
            }
            if (player != turn) {
                throw new GameException("It is not " + player + "'s turn");
            }
            try {
                board = board.place(move, player);
            } catch (IllegalArgumentException e) {
                throw new GameException(e.getMessage());
            }
            advanceState();
            terminal = status.isOver();
            GameSnapshot snap = snapshot();
            send(playerX, snap);
            send(playerO, snap);
        }
        if (terminal) {
            finish();
        }
    }

    void join(GameObserver joiner) throws GameException {
        synchronized (this) {
            if (status != GameStatus.WAITING_FOR_OPPONENT) {
                throw new GameException("Game not available to join: " + name);
            }
            playerO = new Player(joiner);
            status = GameStatus.IN_PROGRESS;
            GameSnapshot snap = snapshot();
            send(playerX, snap);
            send(playerO, snap);
        }
    }

    private void advanceState() {
        Optional<Mark> winner = board.winner();
        if (winner.isPresent()) {
            status = winner.get() == Mark.X ? GameStatus.X_WON : GameStatus.O_WON;
        } else if (board.isFull()) {
            status = GameStatus.DRAW;
        } else {
            turn = turn.opponent();
        }
    }

    private GameSnapshot snapshot() {
        return new GameSnapshot(board, status, turn);
    }

    private void send(Player p, GameSnapshot snap) {
        if (p == null) {
            return;
        }
        p.notifier.execute(() -> {
            try {
                p.observer.onStateChange(snap);
            } catch (RemoteException e) {
                System.err.println("[Game " + name + "] observer unreachable: " + e.getMessage());
            }
        });
    }

    private void checkAlive() {
        Player x, o;
        GameStatus st;
        synchronized (this) {
            if (status.isOver()) {
                return;
            }
            st = status;
            x = playerX;
            o = playerO;
        }
        if (!stillAlive(x)) {
            abort(Mark.X);
        } else if (st == GameStatus.IN_PROGRESS && !stillAlive(o)) {
            abort(Mark.O);
        }
    }

    private boolean stillAlive(Player p) {
        if (p == null) {
            return true;
        }
        boolean reachable;
        try {
            p.observer.ping();
            reachable = true;
        } catch (RemoteException e) {
            reachable = false;
        }
        synchronized (this) {
            p.missed = reachable ? 0 : p.missed + 1;
            return p.missed < MAX_MISSED_PINGS;
        }
    }

    private void abort(Mark disconnected) {
        synchronized (this) {
            if (status.isOver()) {
                return;
            }
            status = GameStatus.ABORTED;
            Player survivor = disconnected == Mark.X ? playerO : playerX;
            send(survivor, snapshot());
        }
        finish();
    }

    private void finish() {
        Player o;
        synchronized (this) {
            o = playerO;
        }
        if (monitorTask != null) {
            monitorTask.cancel(false);
        }
        playerX.notifier.shutdown();
        if (o != null) {
            o.notifier.shutdown();
        }
        onTerminated.run();
    }
}
