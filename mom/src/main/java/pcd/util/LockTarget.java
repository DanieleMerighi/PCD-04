package pcd.util;

public class LockTarget {
    public static final LockTarget GLOBAL = new LockTarget("GLOBAL");

    private final String path;

    private LockTarget(String path) {
        this.path = path;
    }

    public LockTarget sub(String subChannelName) {
        return new LockTarget(this.path + "." + subChannelName);
    }

    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return path;
    }
}