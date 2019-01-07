/*
 *
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *   FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *   OR LOSS OR CORRUPTION OF DATA.
 *
 */
package com.hortonworks.hivestudio.hive.internal;


import com.hortonworks.hivestudio.hive.exceptions.BackgroundJobException;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

/*
To be removed and replaced by Backgroud job and workflow framework.
*/

@Deprecated
@Singleton
@Slf4j
public class BackgroundJobController {
  private Map<String, BackgroundJob> jobs = new HashMap<String, BackgroundJob>();
  public void startJob(final String key, Runnable runnable)  {
    if (jobs.containsKey(key)) {
      try {
        interrupt(key);
        jobs.get(key).getJobThread().join();
      } catch (InterruptedException | BackgroundJobException ignored) {
        log.error("Error in starting backgroud job : ", ignored);
      }
    }
    Thread t = new Thread(runnable);
    t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        if (e instanceof BackgroundJobException) {
          jobs.get(key).setJobException((BackgroundJobException) e);
        }
      }
    });
    jobs.put(key, new BackgroundJob(t));
    t.start();
  }

  public Thread.State state(String key) {
    if (!jobs.containsKey(key)) {
      return Thread.State.TERMINATED;
    }

    Thread.State state = jobs.get(key).getJobThread().getState();

    if (state == Thread.State.TERMINATED) {
      jobs.remove(key);
    }

    return state;
  }

  public boolean interrupt(String key) {
    if (!jobs.containsKey(key)) {
      return false;
    }

    jobs.get(key).getJobThread().interrupt();
    return true;
  }

  public boolean isInterrupted(String key) {
    if (state(key) == Thread.State.TERMINATED) {
      return true;
    }

    return jobs.get(key).getJobThread().isInterrupted();
  }

  class BackgroundJob {

    private Thread jobThread;
    private BackgroundJobException jobException;

    public BackgroundJob(Thread jobThread) {
      this.jobThread = jobThread;
    }

    public Thread getJobThread() {
      if(jobException != null) throw jobException;
      return jobThread;
    }

    public void setJobException(BackgroundJobException exception) {
      this.jobException = exception;
    }
  }

}


