package test;

import ids.minishark.Transfer;


public class PersonTransfer extends Transfer<Person>{

    public PersonTransfer(){
        super("Person");
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

    public void update(Person person){
        this.modify(person);
    }

    public void add(Person person){
       this.insert(person);
    }

    public void test() throws IllegalAccessException {
        String sql=this.getSelectAll()+" where id=?";
    }

}
