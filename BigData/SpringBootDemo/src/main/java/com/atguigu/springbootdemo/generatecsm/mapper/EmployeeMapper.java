package com.atguigu.springbootdemo.generatecsm.mapper;

import com.atguigu.springbootdemo.generatecsm.bean.Employee;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * <p>
 * Mapper 接口
 * </p>
 *
 * @author xxx
 * @since 2024-09-24
 */
@Mapper
public interface EmployeeMapper extends BaseMapper<Employee> {
    @Select("select * from employee where id=#{employee.id} or name=#{employee.name} or salary=#{employee.salary}")
    Employee getEmployee(@Param("employee") Employee employee);

    @Insert("insert into employee(id,name,salary) values(#{employee.id},#{employee.name},#{employee.salary})")
    void saveEmployee(@Param("employee") Employee employee);

    @Update("update employee set name=#{employee.name},salary=#{employee.salary} where id=#{employee.id}")
    void updateEmployee(@Param("employee") Employee employee);

    @Delete("delete from employee where id=#{employee.id} or name=#{employee.name}")
    void deleteEmployee(@Param("employee") Employee employee);

    @Select("${sql}")
    List<Employee> selectEmployee(String sql);
}
