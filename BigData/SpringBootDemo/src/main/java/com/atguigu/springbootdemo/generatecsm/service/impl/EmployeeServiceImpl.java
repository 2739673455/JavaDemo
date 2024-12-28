package com.atguigu.springbootdemo.generatecsm.service.impl;

import com.atguigu.springbootdemo.generatecsm.bean.Employee;
import com.atguigu.springbootdemo.generatecsm.mapper.EmployeeMapper;
import com.atguigu.springbootdemo.generatecsm.service.EmployeeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author xxx
 * @since 2024-09-24
 */
@Service
public class EmployeeServiceImpl extends ServiceImpl<EmployeeMapper, Employee> implements EmployeeService {
    @Autowired
    EmployeeMapper employeeMapper;

    @Override
    public Employee getEmployee(Employee employee) {
        return employeeMapper.getEmployee(employee);
    }

    @Override
    public void saveEmployee(Employee employee) {
        employeeMapper.saveEmployee(employee);
    }

    @Override
    public void updateEmployee(Employee employee) {
        employeeMapper.updateEmployee(employee);
    }

    @Override
    public void deleteEmployee(Employee employee) {
        employeeMapper.deleteEmployee(employee);
    }

    @Override
    public List<Employee> selectEmployee(Employee employee) {
        StringBuffer sql = new StringBuffer("select * from employee where name='");
        sql.append(employee.getName() + "'");
        System.out.println(sql);
        return employeeMapper.selectEmployee(sql.toString());
    }
}
