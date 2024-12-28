package com.atguigu.edupublisher.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CourseOrderStatisticsBean {
	String courseId;
	String courseName;
	Long courseCount;
	Long courseOrder;
	Long finalAmount;

}
