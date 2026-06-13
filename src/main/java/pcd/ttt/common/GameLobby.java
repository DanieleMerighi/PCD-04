package pcd.ttt.common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

public interface GameLobby extends Remote {

    String SERVICE_NAME = "ttt-lobby";

    Game createGame(String name, GameObserver observer) throws RemoteException, GameException;
    Game joinGame(String name, GameObserver observer) throws RemoteException, GameException;
    List<String> listGames() throws RemoteException;
}
