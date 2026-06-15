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

    public static LockTarget of(String path) {
        return new LockTarget(path);
    }
    
    public static boolean conflicts(String firstPath, String secondPath) {
        return firstPath.equals(secondPath)
                || firstPath.startsWith(secondPath + ".")
                || secondPath.startsWith(firstPath + ".");
    }

    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return path;
    }
}