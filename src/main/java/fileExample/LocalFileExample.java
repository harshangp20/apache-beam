package fileExample;

import option.Options;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class LocalFileExample {

    public static void main(String[] args) {
        String filePath = "/home/harshangprajapati/Docs/username.csv";

        String outputFilePath = "src/main/resources/data/outputUserName.csv";

        String fileType = ".csv";

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        Pipeline pipeline = Pipeline.create();

        PCollection<String> collection = pipeline.apply(TextIO.read().from(options.getInputFile()));

        collection.apply(TextIO.write().to(options.getOutputFile()).withNumShards(1).withSuffix(options.getExtn()));

        pipeline.run();
    }

}
