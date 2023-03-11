package filter;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

class Filter implements SerializableFunction<String, Boolean> {

    @Override
    public Boolean apply(String input) {
        return input.contains("Dholka");
    }
}

public class FilterExample {

    public static void main(String[] args) {

        String filePath = "/home/harshangprajapati/Docs/userName_pardo.csv";

        String outPutFilePath = "src/main/resources/pardoExe/userName_pardoOP.csv";

        String fileExtn = ".csv";

        String header = "Id,Name,Last Name, City";

        Pipeline pipeline = Pipeline.create();

        PCollection<String> pCollection = pipeline.apply(TextIO.read().from(filePath));

        PCollection<String> stringPCollection = pCollection.apply(org.apache.beam.sdk.transforms.Filter.by(new Filter()));

        stringPCollection.apply(TextIO.write().to(outPutFilePath).withNumShards(1).withHeader(header).withSuffix(fileExtn));

        pipeline.run().waitUntilFinish();

    }

}
