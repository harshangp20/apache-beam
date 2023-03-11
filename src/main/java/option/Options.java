package option;

import org.apache.beam.sdk.options.PipelineOptions;

public interface Options extends PipelineOptions {

    void setInputFile(String file);
    String getInputFile();

    void setOutputFile(String file);
    String getOutputFile();

    void setExtn(String extn);
    String getExtn();

}
