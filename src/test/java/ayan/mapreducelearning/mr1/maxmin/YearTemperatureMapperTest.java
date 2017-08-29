package ayan.mapreducelearning.mr1.maxmin;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class YearTemperatureMapperTest {
        @Mock
        private Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context;
        private YearTemperatureMapper mapper;

        private MapDriver<LongWritable, Text, IntWritable, IntWritable> mapDriver;

        @Before
        public void setUp() {
                mapper = new YearTemperatureMapper();
                mapDriver = new MapDriver<LongWritable, Text, IntWritable, IntWritable>();
                mapDriver.setMapper(mapper);
        }

        @Test
        public void testMap() throws IOException, InterruptedException {
                mapper.map(new LongWritable(0), new Text("1900,1,30"), context);

                verify(context, times(1)).write(new IntWritable(1900),
                                new IntWritable(30));

                verifyNoMoreInteractions(context);
        }

        @Test
        public void processesValidRecord() throws IOException, InterruptedException {
                mapDriver.withInput(new LongWritable(), new Text("2011,9995,24"))
                                .withOutput(new IntWritable(2011), new IntWritable(24))
                                .runTest();
        }
}