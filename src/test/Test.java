package test;


import ids.sharklite.transfer.TransferFactory;


public class Test {


    public static void main(String[] args) throws Exception {
//        DataBase.defaultDataSource(DBS.getBasicDataSource());

//        DataBase dataBase=new DataBase(DBS.getBasicDataSource());
//        dataBase.classLocationsConfig(new String[]{"test.*"});

//        Transfer<Person> personDao = Transfer.getDefault(Person.class);
//        PersonDao personDao = new PersonDao();
//        personDao.setDataSource(DBS.getBasicDataSource());

//        Person p=personDao.findById(2);

//        System.out.println(p.getId()+","+p.getName());
//

//        personDao.delete(p);
//        personDao.deleteById(id);

//        System.out.println(Test.class.getTypeName());
        TransferFactory factory = TransferFactory.newInstance(DBS.getBasicDataSource());
        PersonDao dao = factory.generate(PersonDao.class);
        for (int i = 0; i < 10; i++) {
            Person p = new Person();
            p.setName("ha");
            p.setAge(12 + i);
            dao.insert(p);
//            int id = p.getId();
//            p = dao.findById(id);
            System.out.println(p.getId() + "," + p.getName());
        }


    }


}
