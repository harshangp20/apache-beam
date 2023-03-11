package pardo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

@SuppressWarnings("ALL")
class CustFilter  extends DoFn<String, String> {

    @ProcessElement
    public void processElement(ProcessContext context) {

        String line = context.element();
        String[] arr = line.split(",");
        if(arr[3].equals("Dholka")) {
            context.output(line);
        }

    }

}

public class ParDoExe {

    public static void main(String[] args) {

        String filePath = "/home/harshangprajapati/Docs/userName_pardo.csv";

        String outPutFilePath = "src/main/resources/pardoExe/userName_pardoOP.csv";

        String fileExtn = ".csv";

        String header = "Id,Name,Last Name, City";

        Pipeline pipeline = Pipeline.create();

        PCollection<String> stringPCollection = pipeline.apply(TextIO.read().from(filePath));

        PCollection<String> collection = stringPCollection.apply(ParDo.of(new CustFilter()));

        collection.apply(TextIO.write().to(outPutFilePath).withHeader(header).withNumShards(1).withSuffix(fileExtn));

        pipeline.run();

    }

}
