public class Test08 {
    public static void main(String[] args) {
        Father f = new Father();
        Son s = new Son();
        System.out.println(f.getInfo());
        s.getInfo(1);
        System.out.println("-----------------");
        s.setInfo("大硅谷");
        System.out.println(f.getInfo());
        s.getInfo(1);
    }
}

class Father {
    public String info = "父";

    public void setInfo(String info) {
        this.info = info;
    }

    public String getInfo() {
        return info;
    }
}

class Son extends Father {
    public String info = "子";

    public void setInfo(String info) {
        super.info = info;
    }

    public void getInfo(int i) {
        System.out.println(info);
        System.out.println(super.info);
    }
}