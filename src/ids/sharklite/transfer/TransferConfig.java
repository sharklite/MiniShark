package ids.sharklite.transfer;

import javax.sql.DataSource;

public final class TransferConfig {

    private DataSource dataSource;
    private String leftWrap = "";
    private String rightWrap = "";

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public String getLeftWrap() {
        return leftWrap;
    }

    public void setLeftWrap(String leftWrap) {
        this.leftWrap = leftWrap;
    }

    public String getRightWrap() {
        return rightWrap;
    }

    public void setRightWrap(String rightWrap) {
        this.rightWrap = rightWrap;
    }

}
