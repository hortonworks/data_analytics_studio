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
package com.hortonworks.hivestudio.common.actor;

import com.google.inject.Injector;

import akka.actor.AbstractExtensionId;
import akka.actor.Actor;
import akka.actor.ExtendedActorSystem;
import akka.actor.Extension;
import akka.actor.Props;

/**
 * An Akka Extension to provide access to Spring managed Actor Beans.
 */
public class GuiceAkkaExtension extends
    AbstractExtensionId<GuiceAkkaExtension.AkkaExtensionProvider> {


  /**
   * Is used by Akka to instantiate the Extension identified by this
   * ExtensionId, internal use only.
   */
  @Override
  public AkkaExtensionProvider createExtension(ExtendedActorSystem system) {
    return new AkkaExtensionProvider();
  }

  /**
   * The Extension implementation.
   */
  public static class AkkaExtensionProvider implements Extension {
    private volatile Injector injector;

    /**
     * Used to initialize the Spring application context for the extension.
     * @param injector
     */
    public void initialize(Injector injector) {
      this.injector = injector;
    }

    /**
     * Create a Props for the specified actorClass using the
     * GuiceActorProducer class.
     *
     * @param actorClass  The name of the actor bean to create Props for
     * @return a Props that will create the named actor bean using Spring
     */
    public Props props(Class<? extends Actor> actorClass) {
      return Props.create(GuiceActorProducer.class, injector, actorClass);
    }
  }
}
