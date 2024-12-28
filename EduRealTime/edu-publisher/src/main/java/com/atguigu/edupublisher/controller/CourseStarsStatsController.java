package com.atguigu.edupublisher.controller;

import com.atguigu.edupublisher.bean.*;
import com.atguigu.edupublisher.service.CourseStarsStatsService;
import com.atguigu.edupublisher.util.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
public class CourseStarsStatsController {
	@Autowired
	CourseStarsStatsService courseStarsStatsService;

	@RequestMapping("/course_stars_stats")
	public String courseStarsStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
		if (date == 0)
			date = DateFormatUtil.now();
		List<CourseStarsStatsBean> result = courseStarsStatsService.getCourseStarsStats(date);
		return result.toString();
	}

	// 1.3.1 课程下单情况统计
	@RequestMapping("/course_order_stats")
	public String CourseOrderStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
		if (date == 0) {
			date = DateFormatUtil.now();
		}
		List<CourseOrderStatisticsBean> result = courseStarsStatsService.getCourseOrderStats(date);

		ArrayList<Object> courseList = new ArrayList<>();
		ArrayList<Object> courseOrder = new ArrayList<>();
		ArrayList<Object> finalAmount = new ArrayList<>();


		for (CourseOrderStatisticsBean osb : result) {

			String name = osb.getCourseName();
			while (name.contains("\\")) {
				name = name.replace("\\", "-");
			}
			courseList.add(name);
			courseOrder.add(osb.getCourseOrder());
			finalAmount.add(osb.getFinalAmount());
		}

		String json = "{\"status\": 0,\"data\": {\"categories\": [\"" + StringUtils.join(courseList, "\",\"") + "\"],\n" +
				"    \"series\": [{\"name\": \"下单人数\",\"data\": [" + StringUtils.join(courseOrder, ",") + "]},\n" +
				"      			  {\"name\": \"下单金额\",\"data\": [" + StringUtils.join(finalAmount, ",") + "]}]}}";

		return json;


	}

	// 1.3.1 类别下单情况统计
	@RequestMapping("/course_category_stats")
	public String CourseCategoryStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
		if (date == 0) {
			date = DateFormatUtil.now();
		}
		List<CourseCategoryStatsBean> result = courseStarsStatsService.getCourseCategoryStats(date);

		ArrayList<Object> category = new ArrayList<>();
		ArrayList<Object> categoryCount = new ArrayList<>();
		ArrayList<Object> categoryOrder = new ArrayList<>();
		ArrayList<Object> categorfinalAmount = new ArrayList<>();

		for (CourseCategoryStatsBean csb : result) {
			String name = csb.getCategoryName();
			while (name.contains("\\")) {
				name = name.replace("\\", "-");
			}
			category.add(name);
			categoryCount.add(csb.getCategoryCount());
			categoryOrder.add(csb.getCategoryOrder());
			categorfinalAmount.add(csb.getCategoryFinalAmount());
		}

		String json1 = "{\"status\": 0,\"data\": [{\n" +
				"       \"dimValue\": \""+StringUtils.join(category, "\",\"")+"\",\"indicators\": [{\"name\": \"下单次数\",\"value\": \""+StringUtils.join(categoryCount, ",")+"\"}]},\n" +
				"      {\"dimValue\": \""+StringUtils.join(category, "\",\"")+"\",\"indicators\": [{\"name\": \"下单人数\",\"value\": \""+StringUtils.join(categoryOrder, ",")+"\"}]},\n" +
				"      {\"dimValue\": \""+StringUtils.join(category, "\",\"")+"\",\"indicators\": [{\"name\": \"下单金额\",\"value\": \""+StringUtils.join(categorfinalAmount, ",")+"\"}]}]}";

		return json1;
	}




	// 1.3.1 学科下单情况统计
	@RequestMapping("/course_subject_stats")
	public String CourseSubjectStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
		if (date == 0) {
			date = DateFormatUtil.now();
		}
		List<CourseSubjectStatsBean> result = courseStarsStatsService.getCourseSubjectStats(date);
		ArrayList<Object> subjectName = new ArrayList<>();
		ArrayList<Object> subjectCount = new ArrayList<>();
		ArrayList<Object> subjectOrder = new ArrayList<>();
		ArrayList<Object> subjectfinalAmount = new ArrayList<>();

		for (CourseSubjectStatsBean ssb : result) {


			subjectName.add(ssb.getSubjectName());
			subjectCount.add(ssb.getSubjectCount());
			subjectOrder.add(ssb.getSubjectOrder());
			subjectfinalAmount.add(ssb.getSubjectfinalAmount());

		}
		String json = "{\"status\": 0,\"data\": {\"categories\": [\"" + StringUtils.join(subjectName, "\",\"") + "\"],\n" +
				"    \"series\": [{\"name\": \"下单次数\",\"data\": [" + StringUtils.join(subjectCount, ",") + "]},\n" +
				"      			  {\"name\": \"下单人数\",\"data\": [" + StringUtils.join(subjectOrder, ",") + "]}," +
				"      			  {\"name\": \"下单金额\",\"data\": [" + StringUtils.join(subjectfinalAmount, ",") + "]}]}}";


		return json;
	}
}
