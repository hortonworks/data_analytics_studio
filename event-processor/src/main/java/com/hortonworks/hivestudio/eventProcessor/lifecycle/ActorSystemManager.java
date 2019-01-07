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
package com.hortonworks.hivestudio.eventProcessor.lifecycle;

import com.google.inject.Injector;
import com.hortonworks.hivestudio.common.actor.GuiceAkkaExtension;

import akka.actor.ActorSystem;
import io.dropwizard.lifecycle.Managed;
import lombok.extern.slf4j.Slf4j;

/**
 * Class manages the actor system. Its starts and stops the system wide actor system when the server
 * starts and stops. Other lifecycle managers which uses the actor system should not terminate the actor system.
 */
@Slf4j
public class ActorSystemManager implements Managed {

  private final Injector injector;
  private ActorSystem actorSystem;

  public ActorSystemManager(Injector injector) {
    this.injector = injector;

  }

  @Override
  public void start() throws Exception {
    log.info("Staring the Actor System manager");

    actorSystem = injector.getInstance(ActorSystem.class);
    GuiceAkkaExtension.AkkaExtensionProvider akkaExtensionProvider = injector.getInstance(GuiceAkkaExtension.AkkaExtensionProvider.class);
    akkaExtensionProvider.initialize(injector);

    actorSystem.registerOnTermination(() -> {
      log.info("Actor system terminated.");
    });
  }

  @Override
  public void stop() throws Exception {
    log.info("Terminating the Actor System");
    actorSystem.terminate();
  }
}
