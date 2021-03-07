import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.regions.Regions;
public class main {
    //------------------------------------------Constants-----------------------------------------
    private static final Regions REGION = Regions.US_EAST_1;
    private static final String INSTANCE_TYPE = InstanceType.M4Large.toString();
    private static final String BUCKET_URL = "s3n://nitay-omer-assignment3/";
    private static final String LOGS_FOLDER_NAME = BUCKET_URL + "logs/";


    public static void main(String [] args) {
        if(args.length < 1)
        {
            System.out.println("Syntax error, please enter DPMin");
            return;
        }
        //------------------------------------------Connection-----------------------------------------
        System.out.println("Trying To Connect...");
        AmazonElasticMapReduce mapReduce = connect();
        System.out.println("Connected");
        System.out.println("Configuring Steps and Running Flow...");

        //------------------------------------------SetJob1------------------------------------------
        HadoopJarStepConfig step_conf1 = new HadoopJarStepConfig()
                .withJar(BUCKET_URL + "jars/Collector.jar")
                .withArgs(BUCKET_URL + "biarcs35", BUCKET_URL + "output/output1", args[0]);

        StepConfig step1 = new StepConfig()
                .withName("Job1")
                .withHadoopJarStep(step_conf1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //------------------------------------------SetRestOfFlow------------------------------------------
        String flowID = run_jobs_flow(mapReduce, args[0], step1);

    }
    private static void waitForJobCompletion(AmazonElasticMapReduce mapReduce, String flowID)
    {
        System.out.println("hi");
    }

    private static AmazonElasticMapReduce connect(){
        return AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion(REGION)
                .withCredentials(new ProfileCredentialsProvider())
                .build();
    }
    private static StepConfig configure_step(int num, String DPMin){
        String[] names = {"NA", "Collector", "Creator", "preJoiner", "Joiner"};
        HadoopJarStepConfig step_conf = new HadoopJarStepConfig()
                .withJar(BUCKET_URL + "jars/" + names[num] + ".jar")
                .withArgs(BUCKET_URL + "output/output" + (num-1), BUCKET_URL + "output/output" + num, DPMin);

        return new StepConfig()
                .withName("Job" + num)
                .withHadoopJarStep(step_conf)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
    }

    private static String run_jobs_flow(AmazonElasticMapReduce mapReduce, String DPMin, StepConfig step1){
        System.out.println("Finished Configuring Steps, Trying to configure run config");
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(15)
                .withMasterInstanceType(INSTANCE_TYPE)
                .withSlaveInstanceType(INSTANCE_TYPE)
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("jessica")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        // Create a flow request including all the steps
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Hypernym_Detector35")
                .withInstances(instances)
                .withSteps(step1,
                        configure_step(2, DPMin),
                        configure_step(3, DPMin),
                        configure_step(4, DPMin))
                .withLogUri(LOGS_FOLDER_NAME)
                .withServiceRole("EMRDefaultRole")
                .withJobFlowRole("EMREC2Role")
                .withReleaseLabel("emr-5.20.0");

        System.out.println("Flow created, Trying to Run");

        // Run the flow
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
        return jobFlowId;
    }
}
