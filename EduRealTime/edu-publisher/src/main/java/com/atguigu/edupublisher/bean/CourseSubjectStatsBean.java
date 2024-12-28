package com.atguigu.edupublisher.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CourseSubjectStatsBean {
	String subjectId;
	String subjectName;
	Long subjectCount;
	Long subjectOrder;
	Long subjectfinalAmount;
}
