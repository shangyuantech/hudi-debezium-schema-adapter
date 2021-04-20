package org.apache.hudi.debezium.example;

public class Employee {

    private String name;
    private int age;
    private String[] emails;
    private Employee boss;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String[] getEmails() {
        return emails;
    }

    public void setEmails(String ... emails) {
        this.emails = emails;
    }

    public Employee getBoss() {
        return boss;
    }

    public void setBoss(Employee boss) {
        this.boss = boss;
    }
}
