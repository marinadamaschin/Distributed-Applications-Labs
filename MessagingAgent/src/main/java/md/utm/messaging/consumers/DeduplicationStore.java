package md.utm.messaging.consumers;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public final class DeduplicationStore {
    private final Path path;
    private final Set<String> ids = new HashSet<>();

    public DeduplicationStore(Path p) throws IOException {
        path = p;
        if (Files.exists(p)) ids.addAll(Files.readAllLines(p));
    }

    public synchronized boolean contains(String id) {
        return ids.contains(id);
    }

    public synchronized void markProcessed(String id) throws IOException {
        if (ids.add(id)) {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) Files.createDirectories(parent);
            Files.writeString(path, id + System.lineSeparator(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
    }
}
