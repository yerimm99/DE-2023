import java.io.IOException;
import java.util.*;

public class Emp {
	public int id;
	public int salary;
	public String dept_id;
	public String emp_info;
		
	public Emp(int _id, int _salary, String _dept_id, String _emp_info) {
		this.id = _id;
		this.salary = _salary;
		this.dept_id = _dept_id;
		this.emp_info = _emp_info;
	}
	public String getString()
	{
		return id + "|" + dept_id + "|" + salary + "|" + emp_info;
	}
}
