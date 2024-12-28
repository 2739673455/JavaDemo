package com.atguigu.springbootdemo.generatecsm.bean;

    import com.baomidou.mybatisplus.annotation.IdType;
    import com.baomidou.mybatisplus.annotation.TableId;
    import java.io.Serializable;
    import java.math.BigDecimal;
    import lombok.*;

/**
* <p>
    * 
    * </p>
*
* @author xxx
* @since 2024-09-24
*/
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public class Employee implements Serializable {

    private static final long serialVersionUID = 1L;

            @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    private String name;

    private BigDecimal salary;
}
