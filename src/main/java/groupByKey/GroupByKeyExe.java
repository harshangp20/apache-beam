package groupByKey;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class GroupByKeyExe {

    public static void main(String[] args) {

        String filePath = "/home/harshangprajapati/Docs/groupByKey.csv";

        String outputPath = "src/main/resources/groupByKey/groupByKeyOP.csv";

        String fileType = ".csv";

        Pipeline pipeline = Pipeline.create();

        PCollection<String> pCollection = pipeline.apply(TextIO.read().from(filePath));

        PCollection<KV<String, Integer>> kvpCollection = pCollection.apply(ParDo.of(new StringToKV()));

        PCollection<KV<String, Iterable<Integer>>> kvpCollection1 =
                kvpCollection.apply(GroupByKey.create());

        PCollection<String> stringPCollection = kvpCollection1.apply(ParDo.of(new KVToString()));

        stringPCollection.apply(TextIO.write().to(outputPath).withSuffix(fileType).withNumShards(1).withHeader("Id,Amount"));

        pipeline.run();

    }

}

class StringToKV extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
        String input = context.element();
        String[] arr = input.split(",");
        try {
            int value = Integer.parseInt(arr[3]);
            context.output(KV.of(arr[0], value));
        } catch (NumberFormatException e) {
            // Skip the row if the fourth column is not a valid integer
        }
    }
}

class KVToString extends DoFn<KV<String, Iterable<Integer>>, String> {

    @ProcessElement
    public void processElement(ProcessContext context) {
        String key = context.element().getKey();
        Iterable<Integer> pair = context.element().getValue();

        Integer sum = 0;
        for (Integer integer : pair) {
            sum += integer;
        }

        context.output(key + "," + sum);

    }

}

/*class StringToKV extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
        String input = context.element();
        String[] arr = input.split(",");
        context.output(KV.of(arr[0], Integer.valueOf(arr[3])));

    }

}*/
