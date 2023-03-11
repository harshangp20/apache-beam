package mapElementExample;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MapElementExample {

    public static void main(String[] args) {

        String filePath = "/home/harshangprajapati/Docs/username.csv";
        String outPutFilePath = "src/main/resources/mapElementExe/userNameOP.csv";
        String fileExtn = ".csv";
        Pipeline pipeline = Pipeline.create();

        PCollection<String> collection = pipeline.apply(TextIO.read().from(filePath));

        PCollection<String> stringPCollection = collection.apply(
                MapElements.into(TypeDescriptors.strings()).via(String::toUpperCase));

        stringPCollection.apply(TextIO.write().to(outPutFilePath).withNumShards(1).withSuffix(fileExtn));

        pipeline.run();

    }

}
