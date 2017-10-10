package test;

import ids.minishark.DataBase;
import ids.minishark.Transfer;


public class Test {


    public static void main(String[] args) throws Exception {
        DataBase.defaultDataSource(DBS.getBasicDataSource());

//        DataBase dataBase=new DataBase(DBS.getBasicDataSource());
//        dataBase.classLocationsConfig(new String[]{"test.*"});

//        Transfer<Person> personDao = Transfer.getDefault(Person.class);
        PersonDao personDao = new PersonDao();

//        for(int i=0;i<10;i++){
//            Person p=new Person();
//            p.setName("ha");
//            p.setAge(12+i);
//            personDao.insert(p);
//            int id=p.getId();
//            p=personDao.findById(id);
//            System.out.println(p.getId()+","+p.getName());
//        }
        Person p=personDao.findById(2);

        System.out.println(p.getId()+","+p.getName());
//

//        personDao.delete(p);
//        personDao.deleteById(id);



    }


}
