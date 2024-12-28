package com.atguigu.exer7;

public enum Month {
    JANUARY("一月"),
    FEBRUARY("二月"),
    MARCH("三月"),
    APRIL("四月"),
    MAY("五月"),
    JUNE("六月"),
    JULY("七月"),
    AUGUST("八月"),
    SEPTEMBER("九月"),
    OCTOBER("十月"),
    NOVEMBER("十一月"),
    DECEMBER("十二月");

    private final String description;

    Month(String description) {
        this.description = description;
    }

    public static Month of(int value) {
        return Month.values()[value - 1];
    }

    public int getValue() {
        return ordinal() + 1;
    }

    public String getDescription() {
        return description;
    }

    public int length(boolean leapYear) {
        if (this == FEBRUARY) {
            return leapYear ? 29 : 28;
        } else {
            switch (this) {
                case JANUARY, MARCH, MAY, JULY, AUGUST, OCTOBER, DECEMBER:
                    return 31;
                default:
                    return 30;
            }
        }
    }
}

