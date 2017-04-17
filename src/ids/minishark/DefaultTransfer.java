package ids.minishark;

class DefaultTransfer<T> extends Transfer<T>{

    DefaultTransfer(Class<T> eClass,String table){
       super(eClass,table);
    }
    DefaultTransfer(Class<T> eClass){
       super(eClass,null);
    }

}
