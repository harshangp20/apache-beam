package count;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class CountPCollection {

    public static void main(String[] args) {
        String filePath = "/home/harshangprajapati/Docs/userName_pardo.csv";

        Pipeline pipeline = Pipeline.create();
        PCollection<String> stringPCollection = pipeline.apply(TextIO.read().from(filePath));
        PCollection<Long> longPCollection = stringPCollection.apply(Count.globally());
        longPCollection.apply(ParDo.of(new DoFn<Long, Void>() {

            @ProcessElement
            public void processElement(ProcessContext processElement) {
                System.out.println("File Count is:" + processElement.element());
            }
        }));

        pipeline.run();

    }

}
