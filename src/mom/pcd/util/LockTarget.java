package pcd.util;

/**
 * Represents a hierarchical identifier for a distributed lock.
 * <p>
 * Lock targets are defined by string paths separated by dots (e.g., "GLOBAL.database.table").
 * This hierarchical structure allows the middleware to evaluate conflicts between parent and child resources.
 */
public class LockTarget {
    /** The root lock target representing the entire global system. */
    public static final LockTarget GLOBAL = new LockTarget("GLOBAL");

    private final String path;

    private LockTarget(String path) {
        this.path = path;
    }

    /**
     * Creates a new child lock target nested under the current target.
     *
     * @param subChannelName the name of the child node
     * @return a new {@code LockTarget} representing the concatenated path
     */
    public LockTarget sub(String subChannelName) {
        return new LockTarget(this.path + "." + subChannelName);
    }

    /**
     * Creates a lock target from an explicit string path.
     *
     * @param path the full hierarchical path of the lock
     * @return a new {@code LockTarget} instance
     */
    public static LockTarget of(String path) {
        return new LockTarget(path);
    }

    /**
     * Evaluates if two hierarchical paths conflict.
     * <p>
     * A conflict occurs if the paths are identical, or if one path is a direct ancestor or descendant of the other.
     *
     * @param firstPath the first lock path to compare
     * @param secondPath the second lock path to compare
     * @return {@code true} if the paths conflict, {@code false} otherwise
     */
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