package join;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

public class InnerJoinExe {

    public static void main(String[] args) {
        String filePathOfUserOrderCSV = "/home/harshangprajapati/Docs/user_order.csv";
        String filePathOfUserCSV = "/home/harshangprajapati/Docs/p_user.csv";

        String fileOutputPath = "src/main/resources/innerJoin/innerJoinExp.csv";

        Pipeline pipeline = Pipeline.create();

        PCollection<KV<String, String>> pOrderCollection = pipeline
                .apply(TextIO.read().from(filePathOfUserOrderCSV))
                .apply(ParDo.of(new OrderParsing()));

        PCollection<KV<String, String>> pUserCollection = pipeline
                .apply(TextIO.read().from(filePathOfUserCSV))
                .apply(ParDo.of(new UserParsing()));

        TupleTag<String> orderTuple = new TupleTag<>();

        TupleTag<String> userTuple = new TupleTag<>();

        PCollection<KV<String , CoGbkResult>> kvpCollection = KeyedPCollectionTuple
                .of(orderTuple,pOrderCollection)
                .and(userTuple,pUserCollection)
                .apply(CoGroupByKey.create());

        PCollection<String > outputPCollection = kvpCollection.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {

            @ProcessElement
            public void processElement(ProcessContext context) {

                String key = context.element().getKey();
                CoGbkResult value = context.element().getValue();

                Iterable<String > orderTable = value.getAll(orderTuple);
                Iterable<String> userTable = value.getAll(userTuple);

                for (String order: orderTable) {
                    for(String user: userTable) {
                        context.output(key + "," + order+"," + user);

                    }
                    
                }

            }

        }));

        outputPCollection.apply(TextIO.write().to(fileOutputPath).withNumShards(1).withSuffix(".csv").withHeader("key,order,user"));

        pipeline.run();

    }

}

class OrderParsing extends DoFn<String, KV<String , String>> {

    @ProcessElement
    public void processElement(ProcessContext context) {
        String[] arr = context.element().split(",");
        String key = arr[0];
        String value = arr[1] + "," + arr[2] + "," + arr[3];

        context.output(KV.of(key,value));
    }

}

class UserParsing extends DoFn<String, KV<String , String>> {

    @ProcessElement
    public void processElement(ProcessContext context) {
        String[] arr = context.element().split(",");
        String key = arr[0];
        String value = arr[1];

        context.output(KV.of(key,value));

    }

}
