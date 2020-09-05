package test;


import ids.sharklite.transfer.Transfer;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;

public class PersonDao extends Transfer<Person> {


    public PersonDao(DataSource dataSource) {
        super(dataSource);
    }

    public Person findById(int id) throws SQLException {
        Person person=new Person();
        person.setId(id);
        this.update(person);
        return this.select(person);
    }

    public void deleteById(int id) throws SQLException {
        Person person=new Person();
        person.setId(id);
        this.delete(person);
    }
    public void insert(Person person) throws SQLException {
//        person.setId(this.tryInt("select SEQ.nextval from dual"));// for oracle
        super.insert(person);
    }



    public List<Person> queryOld() {
        Calendar calendar=Calendar.getInstance();
        calendar.set(Calendar.YEAR,1986);
        calendar.set(Calendar.MONTH,Calendar.JANUARY);
        calendar.set(Calendar.DAY_OF_MONTH,1);
        return null;
//        return this.query(" birthDay <=? ",new Timestamp(calendar.getTimeInMillis()));
    }


}
