package org.wikimedia.analytics.kraken.pig;

//    public class SessionTest extends ClusterMapReduceTestCase {
//        public void test() throws Exception {
//            JobConf conf = createJobConf();
//
//            Path inDir = new Path("testing/jobconf/input");
//            Path outDir = new Path("testing/jobconf/output");
//
//            OutputStream os = getFileSystem().create(new Path(inDir, "text.txt"));
//            Writer wr = new OutputStreamWriter(os);
//            wr.write("b a\n");
//            wr.close();
//
//            conf.setJobName("mr");

//            conf.setOutputKeyClass(Text.class);
//            conf.setOutputValueClass(LongWritable.class);
//
//            conf.setMapperClass(WordCountMapper.class);
//            conf.setReducerClass(SumReducer.class);
//
//            FileInputFormat.setInputPaths(conf, inDir);
//            FileOutputFormat.setOutputPath(conf, outDir);

//            assertTrue(JobClient.runJob(conf).isSuccessful());

//            // Check the output is as expected
//            Path[] outputFiles = FileUtil.stat2Paths(
//                    getFileSystem().listStatus(outDir, new Utils.OutputFileUtils.OutputFilesFilter()));
//
//            assertEquals(1, outputFiles.length);
//
//            InputStream in = getFileSystem().open(outputFiles[0]);
//            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
//            assertEquals("a\t1", reader.readLine());
//            assertEquals("b\t1", reader.readLine());
//            assertNull(reader.readLine());
//            reader.close();
//        }
//    }

//    private MiniDFSCluster dfsCluster = null;
//    //private MiniMRCluster mrCluster = null;
//
//    private final Path input = new Path("input");
//    private final Path output = new Path("output");
//
//    @Before
//    public void setUp() throws Exception {
//
//        // make sure the log folder exists,
//        // otherwise the test fill fail
//        new File("test-logs").mkdirs();
//        //
//        System.setProperty("hadoop.log.dir", "test-logs");
//        System.setProperty("javax.xml.parsers.SAXParserFactory",
//                "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");
//        //
//        Configuration conf = new Configuration();
//        //dfsCluster = new MiniDFSCluster();
//        dfsCluster = new MiniDFSCluster(conf, 1, true, null);
//        //dfsCluster.getFileSystem().makeQualified(input);
//        //dfsCluster.getFileSystem().makeQualified(output);
//        //
//        //mrCluster = new MiniMRCluster(1, getFileSystem().getUri().toString(), 1);
//    }
//
//    protected FileSystem getFileSystem() throws IOException {
//        return dfsCluster.getFileSystem();
//    }
//
//    private void createTextInputFile() throws IOException {
//        OutputStream os = getFileSystem().create(new Path(input, "seed.txt"));
//        Writer wr = new OutputStreamWriter(os);
//        wr.write("b a a\n");
//        wr.close();
//        os.close();
//    }
//
//    private JobConf createJobConf() {
//        JobConf conf = new JobConf();
//        conf.setJobName("read seed value test");
//
//
//
//        //conf.setMapperClass(WordCountMapper.class);
//        //conf.setReducerClass(SumReducer.class);
//
////        conf.setInputFormat(TextInputFormat.class);
////        conf.setMapOutputKeyClass(Text.class);
////        conf.setMapOutputValueClass(IntWritable.class);
////        conf.setOutputKeyClass(Text.class);
////        conf.setOutputValueClass(IntWritable.class);
////        conf.setNumMapTasks(1);
////        conf.setNumReduceTasks(1);
////        FileInputFormat.setInputPaths(conf, input);
////        FileOutputFormat.setOutputPath(conf, output);
//        return conf;
//    }
//
//    @Test
//    public void testCount() throws Exception {
//
//        // prepare for test
//        createTextInputFile();
//
//        // run job
//        JobClient.runJob(createJobConf());
//
//        // check the output
////        Path[] outputFiles = FileUtil.stat2Paths(getFileSystem().listStatus(
////                output, new OutputLogFilter()));
////        Assert.assertEquals(1, outputFiles.length);
////        InputStream is = getFileSystem().open(outputFiles[0]);
////        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
////        Assert.assertEquals("a\t2", reader.readLine());
////        Assert.assertEquals("b\t1", reader.readLine());
////        Assert.assertNull(reader.readLine());
////        reader.close();
//    }
//
//    @After
//    public void tearDown() throws Exception {
//        if (dfsCluster != null) {
//            dfsCluster.shutdown();
//            dfsCluster = null;
//        }
////        if (mrCluster != null) {
////            mrCluster.shutdown();
////            mrCluster = null;
////        }
//    }



