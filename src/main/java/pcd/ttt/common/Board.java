package pcd.ttt.common;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;

public final class Board implements Serializable {

    public static final int SIZE = 3;
    private static final int CELLS = SIZE * SIZE;

    private static final int[][] LINES = {
        {0, 1, 2}, {3, 4, 5}, {6, 7, 8},
        {0, 3, 6}, {1, 4, 7}, {2, 5, 8},
        {0, 4, 8}, {2, 4, 6}
    };

    private final Mark[] cells;

    public Board() {
        this.cells = new Mark[CELLS];
    }

    private Board(Mark[] cells) {
        this.cells = cells;
    }

    public Optional<Mark> cellAt(int row, int col) {
        return Optional.ofNullable(cells[index(row, col)]);
    }

    public Board place(Move move, Mark mark) {
        int i = index(move.row(), move.col());
        if (cells[i] != null) {
            throw new IllegalArgumentException("Cell already occupied: " + move);
        }
        Mark[] copy = cells.clone();
        copy[i] = mark;
        return new Board(copy);
    }

    public boolean isFull() {
        return Arrays.stream(cells).allMatch(c -> c != null);
    }

    public Optional<Mark> winner() {
        return Arrays.stream(LINES)
            .filter(line -> cells[line[0]] != null
                && cells[line[0]] == cells[line[1]]
                && cells[line[1]] == cells[line[2]])
            .map(line -> cells[line[0]])
            .findFirst();
    }

    private static int index(int row, int col) {
        if (row < 0 || row >= SIZE || col < 0 || col >= SIZE) {
            throw new IllegalArgumentException("Coordinates outside the board: (" + row + ", " + col + ")");
        }
        return row * SIZE + col;
    }
}
