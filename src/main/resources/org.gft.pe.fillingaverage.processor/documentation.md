<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  ~
  -->

## FillingAverage

<p align="center"> 
    <img src="icon.png" width="150px;" class="pe-image-documentation"/>
</p>

***

## Description
If there are no readings of values for a certain period of time (delta), it adds to the output the average value of the data received up to that moment (present in a list with user-defined size [nMax]) and the average timestamp of the last two values received

***

## Required input
There should be a field to monitor in the event at certain times, if the timestamp difference is greater than a certain threshold (provided as input), the average of the values is calculated and returned together with the average timestamp

***

## Configuration
###Value to Observe
Specifies the value field that should be monitored.

###Timestamp
Specify the timestamp.

###Maximum number value
Specify the maximum number of values that the list to be averaged must contain.

###Time Window Length (Millisecond)
Specifies the size of the time window in milliseconds beyond which to output the average.


## Output
It emits events with the average value calculated on a list of previously received values, if the timestamp difference is greater than the defined threshold, in order to fill any detection gaps.
