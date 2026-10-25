package md.utm.pad;
import java.util.*;

public final class EmployeeStore {
    public record Employee(int id, String name) {}
    public record Change(long sequence, Employee employee) {}
    private final TreeMap<Integer,Employee> employees = new TreeMap<>();
    private final List<Change> changes = new ArrayList<>();
    private long sequence;
    public EmployeeStore() {
        employees.put(1,new Employee(1,"Ana")); employees.put(2,new Employee(2,"Ion"));
        employees.put(3,new Employee(3,"Maria"));
    }
    public synchronized List<Employee> list(int offset,int limit) {
        return employees.values().stream().skip(offset).limit(limit).toList();
    }
    public synchronized Employee get(int id) { return employees.get(id); }
    public synchronized void put(Employee employee) { employees.put(employee.id(),employee); }
    public synchronized long propose(Employee employee) {
        changes.add(new Change(++sequence,employee)); return sequence;
    }
    public synchronized List<Change> changes(long after) {
        return changes.stream().filter(c -> c.sequence()>after).toList();
    }
}
