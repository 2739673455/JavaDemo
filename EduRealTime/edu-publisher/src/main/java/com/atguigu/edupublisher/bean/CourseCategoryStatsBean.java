package com.atguigu.edupublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CourseCategoryStatsBean {
	String categoryId;
	String categoryName;
	Long categoryCount;
	Long categoryOrder;
	Long categoryFinalAmount;

}
