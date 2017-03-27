package azkaban.jobtype.extend;

import azkaban.jobtype.javautils.AbstractHadoopJob;
import azkaban.utils.Props;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GeneralHadoopJob extends AbstractHadoopJob {
    private static final Logger logger = Logger.getLogger(GeneralHadoopJob.class);

    private static Set<String> MARKED_KEY = new HashSet<String>();

    static {
        MARKED_KEY.add("input.path");
        MARKED_KEY.add("output.path");
        MARKED_KEY.add("force.output.overwrite");
    }

    public GeneralHadoopJob(String name, Props props) {
        super(name, props);
    }

    public void run() throws Exception {
        Props props = this.getProps();
        //set up conf
        JobConf jobConf = getJobConf();

        if (props.containsKey("mapred.mapper.class")) {
            jobConf.setJarByClass(props.getClass("mapred.mapper.class"));
        } else if (props.containsKey("mapreduce.map.class")) {
            jobConf.setJarByClass(props.getClass("mapreduce.map.class"));
        } else {
            throw new IllegalArgumentException("Can't find map class.Please set mapreduce.map.class " +
                    "or mapred.mapper.class");
        }

        FileInputFormat.addInputPath(jobConf, new Path(props.getString("input.path")));
        logger.info("Input path:" + props.getString("input.path"));
        FileOutputFormat.setOutputPath(jobConf, new Path(props.get("output.path")));
        logger.info("Output path:" + props.getString("output.path"));

        if (props.getBoolean("force.output.overwrite")) {
            FileSystem fs = FileSystem.get(jobConf);
            Trash trash = new Trash(fs, jobConf);

            Path outputPath = new Path(props.get("output.path"));
            if (fs.exists(outputPath)) {
                if (outputPath.toUri().getPath().split("/").length < 4) {
                    throw new RuntimeException("OutputPath: " + outputPath + " can't be delete,because you probable " +
                            "delete somebody else's data.");
                }
                logger.info("Move " + outputPath + " to trash.");
                trash.moveToTrash(outputPath);
            }
        }

        //set other map-reduce job config parameters
        for (Map.Entry<Object, Object> entry : props.toProperties().entrySet()) {
            if (!MARKED_KEY.contains(entry.getKey())) {
                jobConf.set((String) entry.getKey(), (String) entry.getValue());
            }
        }

        super.run();
    }
}