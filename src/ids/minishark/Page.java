package ids.minishark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static ids.minishark.TransferExecutor.invokePreparedStatement;

public class Page {
    private int startIndex = 1;
    private int pageRows = 15;
    private int currentPage = 1;
    private Object[] sqlArgs;
    private String sqlSelect;
    private int totalRows = -1;

    Object[] getSqlArgs() {
        if (sqlArgs == null)
            sqlArgs = new Object[0];
        return sqlArgs;
    }

    public void setSqlArgs(Object[] sqlArgs) {
        this.sqlArgs = sqlArgs;
    }


    public int getTotalPages() {
        int totalPages = totalRows / pageRows;
        if (totalRows % pageRows != 0) {
            totalPages++;
        }
        return totalPages;
    }

    public String getSqlSelect() {
        return sqlSelect;
    }

    public void setSqlSelect(String sqlSelect) {
        this.sqlSelect = sqlSelect;
    }


    public int getTotalRows() {
        return totalRows;
    }

    int getStartIndex() {
        return startIndex;
    }

    private void setStartIndex(int startIndex) {
        this.startIndex = startIndex;
        if (this.startIndex < 1)
            this.startIndex = 1;
    }

    public int getPageRows() {
        return pageRows;
    }

    public void setPageRows(int pageRows) {
        this.pageRows = pageRows;
        if (this.pageRows < 1)
            this.pageRows = 1;
        this.setStartIndex((this.currentPage - 1) * this.pageRows + 1);
    }

    public int getCurrentPage() {
        return currentPage;
    }

    public void setCurrentPage(int currentPage) {
        this.currentPage = currentPage;
        if (this.currentPage < 1)
            this.currentPage = 1;
        this.setStartIndex((this.currentPage - 1) * this.pageRows + 1);
    }

    void doPageQuery(Connection conn) {
        PreparedStatement pst = null;
        ResultSet rs = null;
        try {
            pst = conn.prepareStatement(this.sqlSelect, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            if (sqlArgs != null)
                for (int i = 0; i < sqlArgs.length; i++)
                    invokePreparedStatement(pst, (i + 1), sqlArgs[i]);
            rs = pst.executeQuery();
            if (rs.last()) {
                this.totalRows = rs.getRow();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.print(this.getClass().getName() + "count row error :\n" + this.sqlSelect);
        } finally {
            DataBase.close(rs);
            DataBase.close(pst);
            DataBase.close(conn);
        }
    }
}
