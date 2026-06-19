package pcd.ttt.common;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface GameObserver extends Remote {
    void onStateChange(GameSnapshot snapshot) throws RemoteException;

    void ping() throws RemoteException;
}
