/*
 * Copyright 2013 Proofpoint Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.kairosdb.core.aggregator;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.aggregator.annotation.AggregatorName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Converts all longs to double. This will cause a loss of precision for very large long values.
 */
@AggregatorName(name = "apdex", description = "Apdex calculation with T of 0.5.")
public class ApdexAggregator extends RangeAggregator
{
        public static final Logger logger = LoggerFactory.getLogger(ApdexAggregator.class);


        @Override
        protected RangeSubAggregator getSubAggregator()
        {
                return (new ApdexDataPointAggregator());
        }

        private class ApdexDataPointAggregator implements RangeSubAggregator
        {

                @Override
                public Iterable<DataPoint> getNextDataPoints(long returnTime, Iterator<DataPoint> dataPointRange)
                {
                        float sCounter = 1;
                        float tCounter = 0;
                        double val = 0;
                        double apdexThreshold = 500000;
                        float apdexValue = 0;
                        while (dataPointRange.hasNext())
                        {
                                val = dataPointRange.next().getDoubleValue();
                                if (val > apdexThreshold)
                                {
                                        tCounter ++;
                                }
                                else
                                {
                                        sCounter ++;
                                }
                        }
                        apdexValue = ( ( sCounter + (tCounter/2)) / (sCounter + tCounter) );

                        return Collections.singletonList(new DataPoint(returnTime, apdexValue));
                }
        }
}

