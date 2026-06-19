package pcd.ttt.client;

import pcd.ttt.common.*;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PlayerApp {

    private static final int MOVE_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 500;

    private final Scanner in = new Scanner(System.in);
    private final Object lock = new Object();
    private final Queue<String> input = new ArrayDeque<>();
    private final ExecutorService inputReader = Executors.newSingleThreadExecutor();
    private Mark myMark;
    private GameSnapshot latest;

    public static void main(String[] args) {
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "5000");

        String host = args.length > 0 ? args[0] : null;
        new PlayerApp().run(host);
    }

    private void run(String host) {
        GameLobby lobby;
        try {
            Registry registry = LocateRegistry.getRegistry(host);
            lobby = (GameLobby) registry.lookup(GameLobby.SERVICE_NAME);
        } catch (RemoteException | NotBoundException e) {
            System.out.println("Could not reach the server. Is it running?");
            return;
        }

        GameObserver observer;
        GameObserver observerStub;
        try {
            observer = new GameObserverImpl(this::onUpdate);
            observerStub = (GameObserver) UnicastRemoteObject.exportObject(observer, 0);
        } catch (RemoteException e) {
            System.out.println("Could not initialise the client.");
            return;
        }

        try {
            Game game = chooseGame(lobby, observerStub);
            GameSnapshot finalSnap = playLoop(game);
            announce(finalSnap);
        } catch (RemoteException e) {
            System.out.println("Connection to the server lost. The game cannot continue.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            shutdown(observer);
        }
        System.exit(0);
    }

    private void shutdown(GameObserver observer) {
        inputReader.shutdownNow();
        try {
            UnicastRemoteObject.unexportObject(observer, true);
        } catch (RemoteException ignored) {
        }
    }

    private Game chooseGame(GameLobby lobby, GameObserver observerStub) {
        while (true) {
            System.out.print("\n(c) create  (j) join  (l) list > ");
            String cmd = in.nextLine().trim();
            try {
                switch (cmd) {
                    case "c" -> {
                        System.out.print("Game name: ");
                        String name = in.nextLine().trim();
                        Game g = lobby.createGame(name, observerStub);
                        myMark = Mark.X;
                        System.out.println("Created '" + name + "'. You are X.");
                        return g;
                    }
                    case "j" -> {
                        System.out.print("Game name: ");
                        String name = in.nextLine().trim();
                        Game g = lobby.joinGame(name, observerStub);
                        myMark = Mark.O;
                        System.out.println("Joined '" + name + "'. You are O.");
                        return g;
                    }
                    case "l" -> {
                        List<String> games = lobby.listGames();
                        System.out.println(games.isEmpty() ? "(no games)" : games);
                    }
                    default -> System.out.println("Invalid command.");
                }
            } catch (GameException e) {
                System.out.println("Error: " + e.getMessage());
            } catch (RemoteException e) {
                System.out.println("Server unreachable, please try again.");
            }
        }
    }

    private GameSnapshot playLoop(Game game) throws RemoteException, InterruptedException {
        startInputReader();
        System.out.println("Waiting for the game to start...");
        GameSnapshot snap = awaitChange(null);
        while (true) {
            render(snap);
            if (snap.status().isOver()) {
                return snap;
            }
            if (snap.turn() == myMark) {
                Move move = readMyMove(snap);
                if (move == null) {
                    snap = current();
                    continue;
                }
                try {
                    sendMove(game, move);
                    snap = awaitChange(snap);
                } catch (GameException e) {
                    System.out.println("Move rejected: " + e.getMessage());
                    snap = current();
                }
            } else {
                System.out.println("Waiting for the opponent...");
                snap = awaitChange(snap);
            }
        }
    }

    private void sendMove(Game game, Move move) throws RemoteException, GameException {
        for (int attempt = 1; ; attempt++) {
            try {
                game.makeMove(move);
                return;
            } catch (RemoteException e) {
                if (attempt >= MOVE_ATTEMPTS) {
                    throw e;
                }
                System.out.println("Network problem, retrying (" + attempt + "/" + MOVE_ATTEMPTS + ")...");
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    private GameSnapshot awaitChange(GameSnapshot shown) throws InterruptedException {
        synchronized (lock) {
            while (latest == shown) {
                lock.wait();
            }
            return latest;
        }
    }

    private GameSnapshot current() {
        synchronized (lock) {
            return latest;
        }
    }

    private void onUpdate(GameSnapshot snap) {
        synchronized (lock) {
            latest = snap;
            lock.notifyAll();
        }
    }

    private void startInputReader() {
        inputReader.execute(() -> {
            while (in.hasNextLine()) {
                String line = in.nextLine();
                synchronized (lock) {
                    input.add(line);
                    lock.notifyAll();
                }
            }
        });
    }

    private Move readMyMove(GameSnapshot shown) throws InterruptedException {
        while (true) {
            System.out.print("Your move 'row col' (0-2): ");
            String line = awaitInputOrChange(shown);
            if (line == null) {
                return null;
            }
            Move move = parseMove(line);
            if (move != null) {
                return move;
            }
            System.out.println("Invalid input. Example: 1 2");
        }
    }

    private String awaitInputOrChange(GameSnapshot shown) throws InterruptedException {
        synchronized (lock) {
            while (input.isEmpty() && latest == shown) {
                lock.wait();
            }
            return latest != shown ? null : input.poll();
        }
    }

    private Move parseMove(String line) {
        String[] parts = line.trim().split("\\s+");
        try {
            if (parts.length != 2) {
                throw new NumberFormatException();
            }
            return new Move(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private void render(GameSnapshot snap) {
        Board b = snap.board();
        System.out.println();
        for (int r = 0; r < Board.SIZE; r++) {
            StringBuilder row = new StringBuilder();
            for (int c = 0; c < Board.SIZE; c++) {
                row.append(' ').append(b.cellAt(r, c).map(Mark::name).orElse(".")).append(' ');
                if (c < Board.SIZE - 1) row.append('|');
            }
            System.out.println(row);
            if (r < Board.SIZE - 1) System.out.println("---+---+---");
        }
        System.out.println("State: " + snap.status()
                + (snap.status() == GameStatus.IN_PROGRESS ? "  | turn: " + snap.turn() : "")
                + "  | you are: " + myMark);
    }

    private void announce(GameSnapshot snap) {
        String outcome = switch (snap.status()) {
            case X_WON -> myMark == Mark.X ? "You win!" : "You lose.";
            case O_WON -> myMark == Mark.O ? "You win!" : "You lose.";
            case DRAW -> "Draw.";
            case ABORTED -> "Opponent disconnected. Game aborted.";
            default -> "Game interrupted.";
        };
        System.out.println("== " + outcome + " ==");
    }
}
