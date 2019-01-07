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


package com.hortonworks.hivestudio.hive;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.google.inject.Injector;
import com.hortonworks.hivestudio.common.actor.GuiceAkkaExtension;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.hive.actor.OperationController;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ConnectionSystem {

  public static final String ACTOR_SYSTEM_NAME = "HiveViewActorSystem";
  private static Map<String, ActorRef> operationControllerMap = new ConcurrentHashMap<>();
  private final Configuration configuration;

  // credentials map stores usernames and passwords
  private static Map<String, String> credentialsMap = new ConcurrentHashMap<>();
  private final ActorSystem actorSystem;
  private GuiceAkkaExtension.AkkaExtensionProvider extension;

  public ConnectionSystem(Configuration configuration, Injector injector, ActorSystem actorSystem) {
    extension = injector.getInstance(GuiceAkkaExtension.AkkaExtensionProvider.class);
    extension.initialize(injector);
    this.configuration = configuration;
    this.actorSystem = actorSystem;
  }

//  public static ConnectionSystem getInstance() {
//    if (instance == null) {
//      synchronized (lock) {
//        if (instance == null) {
//          instance = new ConnectionSystem(this.akkaProperties);
//        }
//      }
//    }
//    return instance;
//  }

  private ActorRef createOperationController(HiveContext context, Configuration configuration) {
    return actorSystem.actorOf(extension.props(OperationController.class));
  }

  public ActorSystem getActorSystem() {
    return actorSystem;
  }

  /**
   * Returns one operationController per View Instance
   *
   * @param hiveContext
   * @return operationController Instance
   */
  public synchronized ActorRef getOperationController(HiveContext hiveContext) {
    ActorRef ref = operationControllerMap.computeIfAbsent(
        hiveContext.getUsername(),
        k -> createOperationController(hiveContext, configuration));
    return ref;
  }
//
//  public synchronized void persistCredentials(String user,String password){
//    if(!Strings.isNullOrEmpty(password)){
//      credentialsMap.put(user,password);
//    }
//  }


  public synchronized Optional<String> getPassword(HiveContext hiveContext){
    String pass = credentialsMap.get(hiveContext.getUsername());
    return Optional.ofNullable(pass);
  }
//
//  public void removeOperationControllerFromCache(String viewInstanceName) {
//    Map<String, ActorRef> refs = operationControllerMap.remove(viewInstanceName);
//    if (refs != null) {
//      for (ActorRef ref : refs.values()) {
//        Inbox inbox = Inbox.create(getActorSystem());
//        inbox.send(ref, PoisonPill.getInstance());
//      }
//    }
//  }

  public void shutdown() {
      actorSystem.terminate();
  }
}
