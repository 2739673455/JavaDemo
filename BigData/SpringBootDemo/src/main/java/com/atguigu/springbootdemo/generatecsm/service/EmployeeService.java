package com.atguigu.springbootdemo.generatecsm.service;

import com.atguigu.springbootdemo.generatecsm.bean.Employee;
import com.baomidou.mybatisplus.extension.service.IService;

import java.util.List;

/**
 * <p>
 * 服务类
 * </p>
 *
 * @author xxx
 * @since 2024-09-24
 */
public interface EmployeeService extends IService<Employee> {
    Employee getEmployee(Employee employee);

    void saveEmployee(Employee employee);

    void updateEmployee(Employee employee);

    void deleteEmployee(Employee employee);

    List<Employee> selectEmployee(Employee employee);

}
