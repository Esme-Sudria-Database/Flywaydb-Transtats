package db.migration;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.migration.jdbc.JdbcMigration;

import java.io.*;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;

public class T20150925_1900__import_csv implements JdbcMigration {
    public void migrate(Connection connection) throws Exception {

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream csv_input = loader.getResourceAsStream("18699249_ONTIME.csv");
        try {
            BufferedReader csv_reader = new BufferedReader(new InputStreamReader(csv_input));

            String csv_line;
            csv_line = csv_reader.readLine(); // Remove header line
            int i = 0;
            String sql = "INSERT INTO transtats(FL_DATE,UNIQUE_CARRIER,AIRLINE_ID,CARRIER,TAIL_NUM," +
                    "FL_NUM,ORIGIN_AIRPORT_ID,ORIGIN_AIRPORT_SEQ_ID,ORIGIN_CITY_MARKET_ID,ORIGIN,ORIGIN_CITY_NAME,ORIGIN_STATE_ABR," +
                    "ORIGIN_STATE_NM,ORIGIN_WAC,DEST_AIRPORT_ID,DEST_AIRPORT_SEQ_ID,DEST_CITY_MARKET_ID,DEST," +
                    "DEST_CITY_NAME,DEST_STATE_ABR,DEST_STATE_NM,DEST_WAC,DEP_TIME,DEP_DELAY,ARR_TIME," +
                    "ARR_DELAY,DISTANCE,CARRIER_DELAY,WEATHER_DELAY,NAS_DELAY,SECURITY_DELAY," +
                    "LATE_AIRCRAFT_DELAY) VALUES " +
                    "(?,?,?,?,?,?,?,?,?,?,?," +
                    "?,?,?,?,?,?,?,?," +
                    "?,?,?,?,?,?,?,?,?,?,?," +
                    "?,?);";
            PreparedStatement statement = connection.prepareStatement(sql);
            try {
                while ((csv_line = csv_reader.readLine()) != null) {
                    i++;

                    String[] columns = csv_line.split(",");
                    for(int j = 0; j < columns.length; j++) {
                        columns[j] = columns[j].replace("\"","");
                    }

                    statement.setDate(1, Date.valueOf(columns[0]));
                    statement.setString(2, columns[1]);
                    statement.setInt(3, parseInt(columns[2]));
                    statement.setString(4, columns[3]);
                    statement.setString(5, columns[4]);
                    statement.setInt(6, parseInt(columns[5].replace("\"", "")));
                    statement.setInt(7, parseInt(columns[6]));
                    statement.setInt(8, parseInt(columns[7]));
                    statement.setInt(9, parseInt(columns[8]));
                    statement.setString(10, columns[9]);
                    statement.setString(11, columns[10]);
                    statement.setString(12, columns[11]);
                    statement.setString(13, columns[12]);
                    statement.setInt(14, parseInt(columns[14]));
                    statement.setInt(15, parseInt(columns[15]));
                    statement.setInt(16, parseInt(columns[16]));
                    statement.setInt(17, parseInt(columns[17]));
                    statement.setString(18, columns[18]);
                    statement.setString(19, columns[19]);
                    statement.setString(20, columns[20]);
                    statement.setString(21, columns[21]);
                    statement.setInt(22, parseInt(columns[23]));
                    statement.setInt(23, parseInt(columns[24]));
                    statement.setDouble(24, parseDouble(columns[25]));
                    statement.setInt(25, parseInt(columns[26]));
                    statement.setDouble(26, parseDouble(columns[27]));
                    statement.setDouble(27, parseDouble(columns[28]));
                    if(columns.length > 29) {
                        statement.setDouble(28, parseDouble(columns[29]));
                        statement.setDouble(29, parseDouble(columns[30]));
                        statement.setDouble(30, parseDouble(columns[31]));
                        statement.setDouble(31, parseDouble(columns[32]));
                        statement.setDouble(32, parseDouble(columns[33]));
                    } else {
                        statement.setDouble(28, 0.00);
                        statement.setDouble(29, 0.00);
                        statement.setDouble(30, 0.00);
                        statement.setDouble(31, 0.00);
                        statement.setDouble(32, 0.00);
                    }
                    statement.execute();
                }
            } catch (Exception e) {
                throw e;
            } finally {
                statement.close();
            }
        } finally {
            csv_input.close();
        }
    }

    private int parseInt(String string) {
        int value = 0;
        try {
            value = Integer.parseInt(string);
        } catch (NumberFormatException e){

        }

        return value;
    }

    private double parseDouble(String string) {
        double value = 0;
        try {
            value = Double.parseDouble(string);
        } catch (NumberFormatException e){

        }

        return value;
    }
}