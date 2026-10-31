package md.utm.pad;

import java.io.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

public final class PersistentEmployeeStore implements EmployeeStore, AutoCloseable {
    private final Path file;
    private final FileChannel lockChannel;
    private final FileLock owner;
    private TreeMap<Integer, Employee> employees = new TreeMap<>();
    private List<Change> proposals = new ArrayList<>();
    private long sequence;

    public PersistentEmployeeStore(Path file) throws IOException {
        this.file = file.toAbsolutePath();
        Files.createDirectories(this.file.getParent());
        lockChannel = FileChannel.open(Path.of(this.file + ".lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        owner = lockChannel.tryLock();
        if (owner == null) { lockChannel.close(); throw new IOException("Storage file already owned by another process"); }
        try {
            if (Files.exists(this.file)) {
                try (var in = new DataInputStream(Files.newInputStream(this.file))) {
                    if (in.readInt() != 0x50414433) throw new IOException("Invalid storage format");
                    sequence = in.readLong();
                    int count = count(in);
                    for (int i = 0; i < count; i++) { var e = readEmployee(in); employees.put(e.id(), e); }
                    count = count(in);
                    for (int i = 0; i < count; i++) proposals.add(new Change(in.readLong(), readEmployee(in)));
                    if (in.read() != -1) throw new IOException("Trailing storage bytes");
                }
            } else {
                employees.put(1, new Employee(1, "Ana"));
                employees.put(2, new Employee(2, "Ion"));
                employees.put(3, new Employee(3, "Maria"));
                save(employees, proposals, sequence);
            }
        } catch (IOException | RuntimeException e) { close(); throw e; }
    }
    private static int count(DataInputStream in) throws IOException {
        int n = in.readInt(); if (n < 0 || n > 1_000_000) throw new IOException("Invalid record count"); return n;
    }
    static Employee readEmployee(DataInputStream in) throws IOException { return new Employee(in.readInt(), in.readUTF()); }
    static void writeEmployee(DataOutputStream out, Employee e) throws IOException { out.writeInt(e.id()); out.writeUTF(e.name()); }

    private void save(Map<Integer, Employee> next, List<Change> changes, long seq) throws IOException {
        Path temp = Files.createTempFile(file.getParent(), "snapshot-", ".tmp");
        try {
            try (var channel = FileChannel.open(temp, StandardOpenOption.WRITE);
                 var out = new DataOutputStream(Channels.newOutputStream(channel))) {
                out.writeInt(0x50414433); out.writeLong(seq); out.writeInt(next.size());
                for (var e : next.values()) writeEmployee(out, e);
                out.writeInt(changes.size());
                for (var c : changes) { out.writeLong(c.sequence()); writeEmployee(out, c.employee()); }
                out.flush(); channel.force(true);
            }
            //fail explicitly if the filesystem cannot atomically replace a snapshot
            Files.move(temp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } finally { Files.deleteIfExists(temp); }
    }
    public synchronized List<Employee> list(int offset, int limit) { return employees.values().stream().skip(offset).limit(limit).toList(); }
    public synchronized Employee get(int id) { return employees.get(id); }
    public synchronized boolean put(Employee e) throws IOException {
        var next = new TreeMap<>(employees); boolean created = next.put(e.id(), e) == null;
        save(next, proposals, sequence); employees = next; return created;
    }
    public synchronized long propose(Employee e) throws IOException {
        var next = new ArrayList<>(proposals); next.add(new Change(sequence + 1, e));
        save(employees, next, sequence + 1); proposals = next; return ++sequence;
    }
    public synchronized List<Change> changes(long after) { return proposals.stream().filter(c -> c.sequence() > after).toList(); }
    public synchronized void health() throws IOException { if (!owner.isValid() || !Files.isReadable(file)) throw new IOException("Storage unavailable"); }
    public void close() throws IOException { try { owner.release(); } finally { lockChannel.close(); } }
}
