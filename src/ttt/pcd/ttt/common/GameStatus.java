package pcd.ttt.common;

public enum GameStatus {
    WAITING_FOR_OPPONENT,
    IN_PROGRESS,
    X_WON,
    O_WON,
    DRAW,
    ABORTED;

    public boolean isOver() {
        return this == X_WON || this == O_WON || this == DRAW || this == ABORTED;
    }
}
