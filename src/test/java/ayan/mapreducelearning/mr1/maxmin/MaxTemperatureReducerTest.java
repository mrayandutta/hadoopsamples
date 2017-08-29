package ayan.mapreducelearning.mr1.maxmin;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class MaxTemperatureReducerTest {
        @Mock
        private Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context;

        private MaxTemperatureReducer reducer;
        private ReduceDriver<IntWritable, IntWritable, IntWritable, IntWritable> reduceDriver;

        @Before
        public void setUp() {
                reducer = new MaxTemperatureReducer();
                reduceDriver = new ReduceDriver<IntWritable, IntWritable, IntWritable, IntWritable>();
                reduceDriver.withReducer(reducer);
        }

        @Test
        public void testReduceIntWritableIterableOfIntWritableContext()
                        throws Exception {
                IntWritable key = new IntWritable(0);
                IntWritable maxTemp = new IntWritable(0);
                List<IntWritable> values = Lists.newArrayList(maxTemp);

                reducer.reduce(key, values, context);

                verify(context, times(1)).write(key, maxTemp);

                verifyNoMoreInteractions(context);
        }

        @Test
        public void testReducer() throws IOException {
                reduceDriver.withInputKey(new IntWritable(1));
                reduceDriver.withInputValues(Arrays.asList(new IntWritable(20),
                                new IntWritable(25)));
                reduceDriver.withOutput(new IntWritable(1), new IntWritable(25));
                reduceDriver.runTest();
        }

}