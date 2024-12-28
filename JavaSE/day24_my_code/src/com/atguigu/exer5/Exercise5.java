package com.atguigu.exer5;

public class Exercise5 {
    public static void main(String[] args) {
        EmployeeService empservice = new EmployeeService();
        empservice.add(new Employee(1, "张三", "男", 9, 999.9));
        empservice.add(new Employee(2, "张四", "女", 19, 9999.9));
        empservice.add(new Employee(3, "张五", "男", 29, 999999.9));
        empservice.add(new Employee(4, "张六", "女", 39, 9999999.9));
        empservice.add(new Employee(5, "张七", "男", 49, 99999999.9));
        empservice.add(new Employee(6, "张八", "男", 59, 99.9));

        System.out.println("所有年龄超过35的员工");
        empservice.get(e -> e.getAge() > 35).forEach(System.out::println);

        System.out.println("所有薪资高于15000的女员工");
        empservice.get((e) -> {
            return e.getSalary() > 15000.0 && "女".equals(e.getGender());
        }).forEach(System.out::println);

        System.out.println("所有编号是偶数的员工");
        empservice.get((e) -> {
            return e.getId() % 2 == 0;
        }).forEach(System.out::println);

        System.out.println("名字是张三的员工");
        empservice.get((e) -> {
            return "张三".equals(e.getName());
        }).forEach(System.out::println);

        System.out.println("所有年龄超过25，薪资低于10000的男员工");
        empservice.get((e) -> {
            return e.getAge() > 25 && e.getSalary() < 10000.0 && "男".equals(e.getGender());
        }).forEach(System.out::println);

    }
}
