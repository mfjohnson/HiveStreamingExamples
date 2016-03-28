import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by mjohnson on 2/18/16.
 */
public class TestMySQLConnection {

    public static void main(String args[]) {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            Connection con = DriverManager.getConnection("jdbc://192.168.1.100", "root", "hadoop");
            String catalog = con.getCatalog();
            System.out.println("Catalog = "+catalog);
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }
}
