package  test;

import ids.minishark.annotation.Column;
import ids.minishark.annotation.JdbcType;
import ids.minishark.annotation.ReadOnly;
import ids.minishark.annotation.Table;
import java.sql.*;

@Table("Person")
public class Person{

    public Person(){
    }

    @Column("dat")
    @JdbcType(Types.BINARY)
    private byte[] dat;

    @ReadOnly
    @Column("id")
    @JdbcType(Types.INTEGER)
    private int id;

    @ReadOnly
    @Column("times")
    @JdbcType(Types.BINARY)
    private byte[] times;

    @Column("age")
    @JdbcType(Types.INTEGER)
    private int age;

    @Column("name")
    @JdbcType(Types.VARCHAR)
    private String name;

    @Column("gender")
    @JdbcType(Types.BIT)
    private boolean gender;

    @Column("birthDay")
    @JdbcType(Types.TIMESTAMP)
    private Timestamp birthDay;


    public void setDat(byte[] dat){
        this.dat=dat;
    }
    public byte[] getDat(){
        return this.dat;
    }
    public void setId(int id){
        this.id=id;
    }
    public int getId(){
        return this.id;
    }
    public void setTimes(byte[] times){
        this.times=times;
    }
    public byte[] getTimes(){
        return this.times;
    }
    public void setAge(int age){
        this.age=age;
    }
    public int getAge(){
        return this.age;
    }
    public void setName(String name){
        this.name=name;
    }
    public String getName(){
        return this.name;
    }
    public void setGender(boolean gender){
        this.gender=gender;
    }
    public boolean getGender(){
        return this.gender;
    }
    public void setBirthDay(Timestamp birthDay){
        this.birthDay=birthDay;
    }
    public Timestamp getBirthDay(){
        return this.birthDay;
    }

}