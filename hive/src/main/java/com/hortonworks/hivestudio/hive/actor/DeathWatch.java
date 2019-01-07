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


package com.hortonworks.hivestudio.hive.actor;

import akka.actor.ActorRef;
import akka.actor.Terminated;
import com.hortonworks.hivestudio.hive.actor.message.HiveMessage;
import com.hortonworks.hivestudio.hive.actor.message.RegisterActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class DeathWatch extends HiveActor {

    private final static Logger LOG =
            LoggerFactory.getLogger(DeathWatch.class);

    @Override
    public void handleMessage(HiveMessage hiveMessage) {
        Object message = hiveMessage.getMessage();
        if(message instanceof RegisterActor){
            RegisterActor registerActor = (RegisterActor) message;
            ActorRef actorRef = registerActor.getActorRef();
            this.getContext().watch(actorRef);
            LOG.info("Registered new actor "+ actorRef);
            LOG.info("Registration for {} at {}", actorRef,new Date());
        }
        if(message instanceof Terminated){
            Terminated terminated = (Terminated) message;
            ActorRef actor = terminated.actor();
            LOG.info("Received terminate for actor "+ actor);
            LOG.info("Termination for {} at {}", actor,new Date());

        }

    }
}
