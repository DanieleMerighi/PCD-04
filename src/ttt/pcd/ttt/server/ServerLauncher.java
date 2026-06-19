package pcd.ttt.server;


import pcd.ttt.common.GameLobby;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class ServerLauncher {

     public static void main(String[] args) throws Exception {
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "5000");

        GameLobbyImpl lobby = new GameLobbyImpl();
        GameLobby stub = (GameLobby) UnicastRemoteObject.exportObject(lobby, 0);

        Registry registry = LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
        registry.rebind(GameLobby.SERVICE_NAME, stub);

        System.out.println("[Server] Lobby registered as '" + GameLobby.SERVICE_NAME
                + "' on registry port " + Registry.REGISTRY_PORT + ". Waiting for players...");
    }
}