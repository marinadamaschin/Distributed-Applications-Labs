package md.utm.pad;

import java.io.IOException;
import java.util.List;

public interface EmployeeStore {
    record Employee(int id, String name) {}
    record Change(long sequence, Employee employee) {}
    List<Employee> list(int offset, int limit) throws IOException;
    Employee get(int id) throws IOException;
    boolean put(Employee employee) throws IOException;
    long propose(Employee employee) throws IOException;
    List<Change> changes(long after) throws IOException;
    void health() throws IOException;
}
