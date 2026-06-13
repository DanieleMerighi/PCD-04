package pcd.ttt.common;

public enum Mark {
    X,
    O;

    public Mark opponent() {
        return this == X ? O : X;
    }
}
