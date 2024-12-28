package com.atguigu.edupublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficNewAndOldVisitorTraffic {
	Long uvCt;
	Long pvCt;
	Double avgOnLineTime;
	Double avgVisitPageNumber;
}
