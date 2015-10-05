package db.migration;

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
                    "?,?,34,2007,12.00,2258,-5.00,773.00,NULL,NULL,NULL," +
                    "NULL,NULL);";
            PreparedStatement statement = connection.prepareStatement(sql);
            try {
                while ((csv_line = csv_reader.readLine()) != null) {
                    i++;

                    String[] columns = csv_line.split(",");

                    statement.setDate(1, Date.valueOf(columns[0]));
                    statement.setString(2, columns[1]);
                    statement.setInt(3, Integer.parseInt(columns[2]));
                    statement.setString(4, columns[3]);
                    statement.setString(5, columns[4]);
                    statement.setInt(6, Integer.parseInt(columns[5].replace("\"", "")));
                    statement.setInt(7, Integer.parseInt(columns[6]));
                    statement.setInt(8, Integer.parseInt(columns[7]));
                    statement.setInt(9, Integer.parseInt(columns[8]));
                    statement.setString(10, columns[9]);
                    statement.setString(11, columns[10]);
                    statement.setString(12, columns[11]);
                    statement.setString(13, columns[12]);
                    statement.setInt(14, Integer.parseInt(columns[13]));
                    statement.setInt(15, Integer.parseInt(columns[14]));
                    statement.setInt(16, Integer.parseInt(columns[15]));
                    statement.setInt(17, Integer.parseInt(columns[16]));
                    statement.setString(18, columns[17]);
                    statement.setString(19, columns[18]);
                    statement.setString(20, columns[19]);
                    statement.setString(21, columns[20]);
                    statement.execute();
                }
            } finally {
                statement.close();
            }
        } finally {
            csv_input.close();
        }
    }
}