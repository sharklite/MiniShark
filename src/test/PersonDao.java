package test;

import ids.minishark.Transfer;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;

public class PersonDao extends Transfer<Person> {

    public PersonDao(){
        super("Person");//在使用的注解的情况下可不用
    }

    public Person findById(int id){
        Person person=new Person();
        person.setId(id);
        return this.query(person);
    }

    public void deleteById(int id){
        Person person=new Person();
        person.setId(id);
        this.delete(person);
    }
    public void insert(Person person){
//        person.setId(this.tryInt("select SEQ.nextval from dual"));// for oracle
        super.insert(person);
    }

    public List<Person> queryOld() {
        Calendar calendar=Calendar.getInstance();
        calendar.set(Calendar.YEAR,1986);
        calendar.set(Calendar.MONTH,Calendar.JANUARY);
        calendar.set(Calendar.DAY_OF_MONTH,1);
        return this.query(" birthDay <=? ",new Timestamp(calendar.getTimeInMillis()));
    }

}
