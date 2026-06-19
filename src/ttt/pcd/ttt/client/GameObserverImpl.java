package pcd.ttt.client;

import pcd.ttt.common.GameObserver;
import pcd.ttt.common.GameSnapshot;

import java.rmi.RemoteException;
import java.util.function.Consumer;

public class GameObserverImpl implements GameObserver {

    private final Consumer<GameSnapshot> onUpdate;

    public GameObserverImpl(Consumer<GameSnapshot> onUpdate) {
        this.onUpdate = onUpdate;
    }

    @Override
    public void onStateChange(GameSnapshot snapshot) throws RemoteException {
        this.onUpdate.accept(snapshot);
    }

    @Override
    public void ping() {
    }
}
