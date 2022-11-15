import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;
import java.io.*;
import java.util.*;
import org.json.JSONObject;
import org.json.JSONArray;

public class GetData {

    static String prefix = "project3.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding 
    // tables in your database
    String userTableName = null;
    String friendsTableName = null;
    String cityTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;

    // DO NOT modify this constructor
    public GetData(String u, Connection c) {
        super();
        String dataType = u;
        oracleConnection = c;
        userTableName = prefix + dataType + "_USERS";
        friendsTableName = prefix + dataType + "_FRIENDS";
        cityTableName = prefix + dataType + "_CITIES";
        currentCityTableName = prefix + dataType + "_USER_CURRENT_CITIES";
        hometownCityTableName = prefix + dataType + "_USER_HOMETOWN_CITIES";
    }

    // TODO: Implement this function
    @SuppressWarnings("unchecked")
    public JSONArray toJSON() throws SQLException {

        // This is the data structure to store all users' information
        JSONArray users_info = new JSONArray();
        
        try (Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
            // Your implementation goes here....
            ResultSet rst = stmt.executeQuery(
                "SELECT * FROM " + userTableName); // sort by users born in that month, descending; break ties by birth month
            
            while (rst.next()) {
                JSONObject user = new JSONObject(); 
                int user_id = rst.getInt(1);
                user.put("user_id",user_id); 
                user.put("first_name", rst.getString(2));
                user.put("last_name", rst.getString(3));
                user.put("YOB", rst.getInt(4));
                user.put("MOB", rst.getInt(5)); 
                user.put("DOB", rst.getInt(6));
                user.put("gender", rst.getString(7));
                try (Statement stmt2 = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {
                    ArrayList<Integer> arrlist= new ArrayList<Integer>();                    
                    ResultSet rst2 = stmt2.executeQuery(
                        "SELECT F.USER2_ID FROM " + friendsTableName + " F" +
                        " WHERE F.USER1_ID = " + user_id + " AND F.USER2_ID > " + user_id 
                        );
                        while (rst2.next()) {
                        arrlist.add(rst2.getInt(1));
                        }
                        user.put("friends", arrlist);
                        stmt2.close();
                }
                try (Statement stmt3 = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {

                    ResultSet rst3 = stmt3.executeQuery(
                            "SELECT C.CITY_NAME, C.STATE_NAME, C.COUNTRY_NAME FROM " + cityTableName + 
                            " C JOIN " + currentCityTableName + " CC ON CC.CURRENT_CITY_ID = C.CITY_ID " +
                            " WHERE CC.USER_ID = " + user_id
                            );
                    JSONObject currentcity = new JSONObject(); 
                    while (rst3.next()) {
                        currentcity.put("country", rst3.getString(3));
                        currentcity.put("city", rst3.getString(1));
                        currentcity.put("state", rst3.getString(2));
                    }
                    user.put("current",currentcity);
                    stmt3.close();

                }
                try (Statement stmt4 = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)) {

                    ResultSet rst4 = stmt4.executeQuery(
                            "SELECT C.CITY_NAME, C.STATE_NAME, C.COUNTRY_NAME FROM " + cityTableName + 
                            " C JOIN " + hometownCityTableName + " CC ON CC.HOMETOWN_CITY_ID = C.CITY_ID " +
                            " WHERE CC.USER_ID = " + user_id
                            );
                    JSONObject hometowncity = new JSONObject(); 
                    while (rst4.next()) {
                        hometowncity.put("country", rst4.getString(3));
                        hometowncity.put("city", rst4.getString(1));
                        hometowncity.put("state", rst4.getString(2));
                    }
                    user.put("hometown",hometowncity);
                    stmt4.close();

                }
                users_info.put(user);
            }
            stmt.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }

        return users_info;
    }

    // This outputs to a file "output.json"
    // DO NOT MODIFY this function
    public void writeJSON(JSONArray users_info) {
        try {
            FileWriter file = new FileWriter(System.getProperty("user.dir") + "/output.json");
            file.write(users_info.toString());
            file.flush();
            file.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
