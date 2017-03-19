package test;

import ids.minishark.DataBase;
import ids.minishark.JavaCreator;

public class Test {


    public static void main(String[] args) throws Exception {
        DataBase.defaultDataSource(DBS.getBasicDataSource());

        String s=JavaCreator.create("test","Person",DBS.getBasicDataSource().getConnection());
        System.out.println(s);
        //PersonTransfer personTransfer =new PersonTransfer();
//        List<Person> list=personTransfer.query("1=1");//new ArrayList<>();
//        for(int i=0;i<50;i++){
//            Person p=new Person();
//            list.add(p);
//            p.setBirthDay(null);
//            p.setId(i);
//        }
//        personTransfer.delete(list);
//        for(Person p:list){
//            System.out.println(p.getId()+","+p.getName());
//        }

    }


}
