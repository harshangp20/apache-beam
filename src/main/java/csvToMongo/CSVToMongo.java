package csvToMongo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PCollection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class CSVToMongo {

    public static void main(String[] args) {

        Pipeline p = Pipeline.create();

        PCollection<String> poutput = p.apply(JdbcIO.<String>read().
                withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/rutwa_uat?useSSL=false")
                        .withUsername("root")
                        .withPassword("Rydot123.?"))
                .withQuery("SELECT name, short_name, address from agency_master WHERE name = ? ")
                .withCoder(StringUtf8Coder.of())
                .withStatementPreparator(new JdbcIO.StatementPreparator() {

                    public void setParameters(PreparedStatement preparedStatement) throws Exception {
                        preparedStatement.setString(1, "Rutwa Toursim L.L.C");
                    }
                })
                .withRowMapper(new JdbcIO.RowMapper<String>() {

                    public String mapRow(ResultSet resultSet) throws Exception {
                        return resultSet.getString(1) + "," + resultSet.getString(2) + "," + resultSet.getString(3);
                    }
                })
        );


        poutput.apply(TextIO.write().to("src/main/resources/jdbcExp/jdbc_output.csv").withNumShards(1).withSuffix(".csv"));

        p.run();

    }

}
