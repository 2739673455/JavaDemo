package com.atguigu.springbootdemo.generatecsm.controller;

import com.atguigu.springbootdemo.generatecsm.bean.Employee;
import com.atguigu.springbootdemo.generatecsm.service.EmployeeService;
import com.baomidou.dynamic.datasource.annotation.DS;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.LocalTime;
import java.util.List;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author xxx
 * @since 2024-09-24
 */
@RestController
@DS("test1")
public class EmployeeController {
    @Autowired
    EmployeeService employeeService;

    @GetMapping(value = "/getemployee")
    public Employee getEmployee(@RequestParam(value = "id", required = false) Long id,
                                @RequestParam(value = "name", required = false, defaultValue = "-") String name,
                                @RequestParam(value = "salary", required = false) BigDecimal salary) {
        Employee employee = new Employee(id, name, salary);
        employee = employeeService.getEmployee(employee);
        System.out.println("select " + employee + " at " + LocalTime.now());
        return employee;
    }

    @GetMapping(value = "/saveemployee")
    public Employee saveEmployee(@RequestParam(value = "id", required = false) Long id,
                                 @RequestParam(value = "name", required = false, defaultValue = "-") String name,
                                 @RequestParam(value = "salary", required = false) BigDecimal salary) {
        Employee employee = new Employee(id, name, salary);
        employeeService.saveEmployee(employee);
        String result = "insert " + employee;
        System.out.println(result + " at " + LocalTime.now());
        return employee;
    }

    @GetMapping(value = "/updateemployee")
    public Employee updateEmployee(@RequestParam(value = "id", required = false) Long id,
                                   @RequestParam(value = "name", required = false, defaultValue = "-") String name,
                                   @RequestParam(value = "salary", required = false) BigDecimal salary) {
        Employee employee = new Employee(id, name, salary);
        employeeService.updateEmployee(employee);
        String result = "update " + employee;
        System.out.println(result + " at " + LocalTime.now());
        return employee;
    }

    @GetMapping(value = "/deleteemployee")
    public Employee deleteEmployee(@RequestParam(value = "id", required = false) Long id,
                                   @RequestParam(value = "name", required = false, defaultValue = "-") String name,
                                   @RequestParam(value = "salary", required = false) BigDecimal salary) {
        Employee employee = new Employee(id, name, salary);
        employeeService.deleteEmployee(employee);
        String result = "delete " + employee;
        System.out.println(result + " at " + LocalTime.now());
        return employee;
    }


    @PostMapping(value = "/getemployee2")
    public Employee getEmployeeMP(@RequestBody Employee employee) {
        employee = employeeService.getById(employee.getId());
        System.out.println("select " + employee + " at " + LocalTime.now());
        return employee;
    }

    @PostMapping(value = "/saveemployee2")
    public String saveEmployeeMP(@RequestBody Employee employee) {
        employeeService.save(employee);
        String result = "insert " + employee;
        System.out.println(result + " at " + LocalTime.now());
        return result;
    }

    @PostMapping(value = "/updateemployee2")
    public String updateEmployeeMP(@RequestBody Employee employee) {
        employeeService.updateById(employee);
        String result = "update " + employee;
        System.out.println(result + " at " + LocalTime.now());
        return result;
    }

    @PostMapping(value = "/deleteemployee2")
    public String deleteEmployeeMP(@RequestBody Employee employee) {
        employeeService.removeById(employee);
        String result = "delete " + employee;
        System.out.println(result + " at " + LocalTime.now());
        return result;
    }

    @PostMapping(value = "/select")
    public List<Employee> selectEmployee(@RequestBody Employee employee) {
        List<Employee> employees = employeeService.selectEmployee(employee);
        System.out.println("select " + employees + " at " + LocalTime.now());
        return employees;
    }
}