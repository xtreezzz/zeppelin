---
layout: page
title: "Running a Notebook on a Given Schedule Automatically"
description: "You can run a notebook on a given schedule automatically by setting up a cron scheduler on the notebook."
group: usage/other_features
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
{% include JB/setup %}

# Running a Notebook on a Given Schedule Automatically

<div id="toc"></div>

Apache Zeppelin provides a cron scheduler for each notebook. You can run a notebook on a given schedule automatically by setting up a cron scheduler on the notebook.

## Setting up a cron scheduler on a notebook

Click the clock icon on the tool bar and open a cron scheduler dialog box.

<img src="{{BASE_PATH}}/assets/themes/zeppelin/img/docs-img/cron_scheduler_dialog_box.png" />

There are the following items which you can input or set:

### Preset

You can set a cron schedule easily by clicking each option such as `1m` and `5m`. The login user is set as a cron executing user automatically. You can also clear the cron schedule settings by clicking `None`.

### Cron expression

You can set the cron schedule by filling in this form. Please see [Cron Trigger Tutorial](http://www.quartz-scheduler.org/documentation/quartz-2.2.x/tutorials/crontrigger) for the available cron syntax.

### Cron executing user (It is removed from 0.8 where it enforces the cron execution user to be the note owner for security purpose)

You can set the cron executing user by filling in this form and press the enter key.

### After execution stop the interpreter

When this checkbox is set to "on", the interpreters which are binded to the notebook are stopped automatically after the cron execution. This feature is useful if you want to release the interpreter resources after the cron execution.

> **Note**: A cron execution is skipped if one of the paragraphs is in a state of `RUNNING` or `PENDING` no matter whether it is executed automatically (i.e. by the cron scheduler) or manually by a user opening this notebook.

### Enable cron

Set property **zeppelin.notebook.cron.enable** to **true** in `$ZEPPELIN_HOME/conf/zeppelin-site.xml` to enable Cron feature.

### Run cron selectively on folders

In `$ZEPPELIN_HOME/conf/zeppelin-site.xml` make sure the property **zeppelin.notebook.cron.enable** is set to **true**, and then set property **zeppelin.notebook.cron.folders** to the desired folder as comma-separated values, e.g. `*yst*, Sys?em, System`. This property accepts wildcard and joker.

## Configure Quartz Settings
By default, Zeppelin uses `quartz.properties` file located in `org.quartz`.

In case you didn't configure quartz.properties:
1. Create file `quartz.properties` from default properties;
2. Add it to `conf` directory;
3. Specify parameters you wish according to quartz docs.

### Configure DynamicThreadPool
  <table class="table-configuration">
    <col width="200">
    <tr>
        <td>Property Name</td>
        <td>Description</td>
        <td>Required</td>
        <td>Type</td>
        <td>Value</td>
    </tr>
    <tr>
      <td>```org.quartz.threadPool.class```</td>
      <td>Name of the ThreadPool implementation you wish to use.</td>
      <td>yes</td>
      <td>string (class name)</td>
      <td>```org.apache.zeppelin.scheduler.pool.DynamicThreadPool```</td>
    </tr>
    <tr>
      <td>```org.quartz.threadPool.threadCount```</td>
      <td>Thread Pool Size - the number of threads available for concurrent execution of jobs.</td>
      <td>yes</td>
      <td>int</td>
      <td>Default: 0</td>
    </tr>
    <tr>
      <td>```org.quartz.threadPool.keepAliveTimeSec```</td>
      <td>If the pool currently has more than corePoolSize threads, excess threads will be terminated if they have been idle for more than the keepAliveTime. This provides a means of reducing resource consumption when the pool is not being actively used. If the pool becomes more active later, new threads will be constructed.</td>
      <td>no</td>
      <td>long</td>
      <td>Default: 60</td>
    </tr>
    <tr>
      <td>```org.quartz.scheduler.instanceName```</td>
      <td>Scheduler instance name.</td>
      <td>yes</td>
      <td>string</td>
      <td>Any string</td>
    </tr>
    <tr>
      <td>```org.quartz.scheduler.instanceId```</td>
      <td>Scheduler instance id. You can get pool by ```instanceId```.</td>
      <td>no</td>
      <td>string</td>
      <td>Any string</td>
    </tr>
    <tr>
      <td>```org.quartz.threadPool.threadPriority```</td>
      <td>Priority of threads created by pool. Can be any int between Thread.MIN_PRIORITY (which is 1) and Thread.MAX_PRIORITY (which is 10).</td>
      <td>no</td>
      <td>int</td>
      <td>Default: ```Thread.NORM_PRIORITY (5)```</td>
    </tr>
    <tr>
      <td>```org.quartz.threadPool.threadNamePrefix```</td>
      <td>Thread names correspond to format {threadNamePrefix}-{threadNo}.</td>
      <td>no</td>
      <td>string</td>
      <td>Default:```instanceName```</td>
    </tr>
  </table>
