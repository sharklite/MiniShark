package test;

import ids.sharklite.transfer.annotation.Column;
import ids.sharklite.transfer.annotation.DataType;
import ids.sharklite.transfer.annotation.ReadOnly;
import ids.sharklite.transfer.annotation.Table;
import java.sql.*;

//@Table("Person")
public class Person{

    public Person(){
    }

    @Column("dat")
    @DataType(JDBCType.BINARY)
    private byte[] dat;

    @ReadOnly
    @Column("id")
    @DataType(JDBCType.INTEGER)
    private int id;

    @ReadOnly
    @Column("times")
    @DataType(JDBCType.BINARY)
    private byte[] times;

    @Column("age")
    @DataType(JDBCType.INTEGER)
    private int age;

    @Column("name")
    @DataType(JDBCType.VARCHAR)
    private String name;

    @Column("gender")
    @DataType(JDBCType.BIT)
    private boolean gender;

    @Column("birthDay")
    @DataType(JDBCType.TIMESTAMP)
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