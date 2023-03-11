package mapElementFunctionExample;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

class User extends SimpleFunction<String , String> {

    @Override
    public String apply(String inputString) {

        String[] arr = inputString.split(",");
        String sId = arr[0];
        String uId = arr[1];
        String userName = arr[2];
        String vId = arr[3];
        String duration = arr[4];
        String startTime = arr[5];
        String sex = arr[6];

        String outputString;

        if (sex.equals("1")) {
            outputString = sId + "," + uId + "," + userName + "," + vId + "," + duration + "," + startTime + "," + "M";
        } else if (sex.equals("0")) {
            outputString = sId + "," + uId + "," + userName + "," + vId + "," + duration + "," + startTime + "," + "F";
        } else {
            outputString = inputString;
        }

        return outputString;
    }
}

public class MapElementFunctionExample {

    public static void main(String[] args) {

        String filePath = "/home/harshangprajapati/Docs/userName.csv";
        String outPutFilePath = "src/main/resources/mapElementFunExe/userNameOP.csv";
        String fileExtn = ".csv";

        Pipeline pipeline = Pipeline.create();

        PCollection<String> pCollection = pipeline.apply(
                TextIO.read().from(filePath)
        );

        PCollection<String> stringPCollection = pCollection.apply(MapElements.via(new User()));

        stringPCollection.apply(TextIO.write().to(outPutFilePath).withNumShards(1).withSuffix(fileExtn));

        pipeline.run();

    }

}
