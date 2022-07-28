/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.gft.pe.fillingaverage;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.model.schema.PropertyScope;
import org.apache.streampipes.sdk.builder.PrimitivePropertyBuilder;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.helpers.EpRequirements;
import org.apache.streampipes.sdk.helpers.Labels;
import org.apache.streampipes.sdk.helpers.Locales;
import org.apache.streampipes.sdk.helpers.OutputStrategies;
import org.apache.streampipes.sdk.utils.Datatypes;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;
import org.apache.streampipes.wrapper.standalone.ProcessorParams;
import org.apache.streampipes.wrapper.standalone.StreamPipesDataProcessor;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;


public class FillingAverageDataProcessor extends StreamPipesDataProcessor {

  private String input_value;
  private Double nMax;
  private String timestamp_value;
  private int delta;

  private static final String INPUT_VALUE = "value";
  private static final String NMAX = "nMax";
  private static final String TIMESTAMP_VALUE = "timestamp_value";
  private static final String DELTA = "delta";

  List<Double> listSource = new ArrayList<>();
  List<Double> listTmestamp = new ArrayList<>();

  Double timestamp1 = 0.0;
  Double timestamp2 = 0.0;


  @Override
  public DataProcessorDescription declareModel() {
    return ProcessingElementBuilder.create("org.gft.pe.fillingaverage.processor")
            .withLocales(Locales.EN)
            .category(DataProcessorType.AGGREGATE)

            .requiredStream(StreamRequirementsBuilder.create()
                    .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                            Labels.withId(INPUT_VALUE), PropertyScope.NONE)
                    .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                            Labels.withId(TIMESTAMP_VALUE), PropertyScope.NONE)
                    .build())

            .requiredFloatParameter(Labels.withId(NMAX))
            .requiredIntegerParameter(Labels.withId(DELTA))

            .outputStrategy(OutputStrategies.append(PrimitivePropertyBuilder.create(Datatypes.Double, "outputValue").build()
                    ,PrimitivePropertyBuilder.create(Datatypes.String, "calculated_timestamp").build()))

            .build();
  }

  @Override
  public void onInvocation(ProcessorParams processorParams,
                            SpOutputCollector out,
                            EventProcessorRuntimeContext ctx) throws SpRuntimeException  {
    this.input_value = processorParams.extractor().mappingPropertyValue(INPUT_VALUE);
    //recovery nMax value
    //maximum size of the list of values to be averaged
    this.nMax = processorParams.extractor().singleValueParameter(NMAX,Double.class);
    this.timestamp_value = processorParams.extractor().mappingPropertyValue(TIMESTAMP_VALUE);
    //recovery delta value
    //time value beyond which the average is to be made
    this.delta = processorParams.extractor().singleValueParameter(DELTA, Integer.class);

  }

  @Override
  public void onEvent(Event event,SpOutputCollector out){

    Double outputValue=0.0;
    double mean =0.0;
    double meanTstamp=0.0;

    //recovery input value
    Double value = event.getFieldBySelector(this.input_value).getAsPrimitive().getAsDouble();

    //recovery timestamp value
    String timestampStr = event.getFieldBySelector(this.timestamp_value).getAsPrimitive().getAsString();

    //if we are in the first event it sets the timestamp1 values
    if (timestamp1 == 0.0) {
      timestamp1 = Double.parseDouble(timestampStr);

      //if we are in the second event it sets the timestamp2 values
    }else if (timestamp2 == 0.0) {
      timestamp2 = Double.parseDouble(timestampStr);

      // if we are in the third event or higher, set the value of timestamp1 = timestamp2 and insert the timestamp of the current event, in timestamp2
    }else{
      timestamp1 = timestamp2;
      timestamp2 = Double.parseDouble(timestampStr);
    }

    //check if the list is null
    if (listSource == null){
      System.out.println("Null list");

    //if the list is empty then add current value
    } else if (listSource.isEmpty()){
      //add value
      listSource.add(value);

      //if the list size is equal to the value inserted in input,
      // then it removes the first value of the list and adds the current value
    } else if (listSource.size() == this.nMax){
      //removes the first value of the list
      listSource.remove(0);
      //adds the current value to the list
      listSource.add(value);

      //if the list size is not equal to the value inserted in input,
      //adds the current value to the list
    } else {
      listSource.add(value);
    }

    //if the list is not empty, performs the average
    if(listSource.size() >= 1){
      //perform mean
      mean = performMeanOperation(listSource);
    }

    //if only the first value was received or the difference between timestamp2 and timestamp1 <= delta
    //then set outputValue=value
    if ((timestamp2 == 0.0) || (timestamp2 - timestamp1 <= delta) ) {
      outputValue=value;


      meanTstamp = Double.parseDouble(timestampStr);


      //if more than one value was received or the difference between timestamp2 and timestamp1 > delta
      //then set outputValue=mean
    }else{
      outputValue=mean;

      //add to the list of timestamp timestamp1 and timestamp2
      listTmestamp.add(timestamp1);
      listTmestamp.add(timestamp2);

      //perform the mean of timestmap list
      meanTstamp = performMeanOperation(listTmestamp);
    }

    //transform timestamp into date format "yyyy-MM-dd hh:mm:ss"
    String strMeanTStamp = dataTransformation(meanTstamp);

    //add to the event output the value and the mean timestamp
    event.addField("outputValue", outputValue);
    event.addField("calculated_timestamp", strMeanTStamp);
    out.collect(event);

    out.collect(event);
  }

  @Override
  public void onDetach(){
  }

  //function to calculate the mean
  //recive in input a list of double and return a double value (average)
  public double performMeanOperation(List<Double> values) {
    DescriptiveStatistics ds = new DescriptiveStatistics();
    values.forEach(ds::addValue);
    double mean = ds.getMean();
    return mean;
  }

  //function that formats the timestamp from milliseconds to date
  public String dataTransformation (double tStamp){
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis((long) tStamp);

    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    return formatter.format(calendar.getTime());

  }


}
