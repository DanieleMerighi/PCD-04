package pcd.ttt.common;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface Game extends Remote {
    void makeMove(Move move) throws RemoteException, GameException;
}
