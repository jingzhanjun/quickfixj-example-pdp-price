/*******************************************************************************
 * Copyright (c) quickfixengine.org  All rights reserved.
 *
 * This file is part of the QuickFIX FIX Engine
 *
 * This file may be distributed under the terms of the quickfixengine.org
 * license as defined by quickfixengine.org and appearing in the file
 * LICENSE included in the packaging of this file.
 *
 * This file is provided AS IS with NO WARRANTY OF ANY KIND, INCLUDING
 * THE WARRANTY OF DESIGN, MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE.
 *
 * See http://www.quickfixengine.org/LICENSE for licensing information.
 *
 * Contact ask@quickfixengine.org if any conditions of this licensing
 * are not clear to you.
 ******************************************************************************/

package quickfix.examples.banzai;

import org.quickfixj.jmx.JmxExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import quickfix.*;
import quickfix.field.*;
import quickfix.fix43.NewOrderSingle;
import quickfix.fix50sp1.MarketDataRequest;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

/**
 * Entry point for the Downstream2 application.
 */
public class Downstream2 {
    private static final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private static final Logger log = LoggerFactory.getLogger(Downstream2.class);
    private static Downstream2 downstream;
    private boolean initiatorStarted = false;
    private static Initiator initiator = null;
    private static BanzaiApplication application=new BanzaiApplication(new OrderTableModel(), new ExecutionTableModel());

    public Downstream2(String[] args) throws Exception {
        InputStream inputStream = null;
        if (args.length == 0) {
            inputStream = Downstream2.class.getResourceAsStream("banzai.cfg");
        } else if (args.length == 1) {
            inputStream = new FileInputStream(args[0]);
        }
        if (inputStream == null) {
            System.out.println("usage: " + Downstream2.class.getName() + " [configFile].");
            return;
        }
        SessionSettings settings = new SessionSettings(inputStream);
        inputStream.close();

        boolean logHeartbeats = Boolean.valueOf(System.getProperty("logHeartbeats", "true"));

        MessageStoreFactory messageStoreFactory = new FileStoreFactory(settings);
        LogFactory logFactory = new ScreenLogFactory(true, true, true, logHeartbeats);
        MessageFactory messageFactory = new DefaultMessageFactory();

        initiator = new SocketInitiator(application, messageStoreFactory, settings, logFactory, messageFactory);

        JmxExporter exporter = new JmxExporter();
        exporter.register(initiator);
    }

    public synchronized void logon() {
        if (!initiatorStarted) {
            try {
                initiator.start();
                initiatorStarted = true;
            } catch (Exception e) {
                log.error("Logon failed", e);
            }
        } else {
            for (SessionID sessionId : initiator.getSessions()) {
                Session.lookupSession(sessionId).logon();
            }
        }
    }

    public void logout() {
        for (SessionID sessionId : initiator.getSessions()) {
            Session.lookupSession(sessionId).logout("user requested");
        }
    }

    public static Downstream2 get() {
        return downstream;
    }

    public static void main(String[] args) throws Exception {
        try {
            downstream = new Downstream2(args);
            downstream.logon();
        } catch (Exception e) {
            log.info(e.getMessage(), e);
        }finally{
//            testMarketDataRequest();
            testNewOrderSingle();
        }

    }

    private static void testNewOrderSingle() throws SessionNotFound {
        NewOrderSingle newOrderSingle = new NewOrderSingle();
        newOrderSingle.set(new ClOrdID("TEST_NewOrderSingle"));
        newOrderSingle.set(new Side('1'));
        LocalDateTime localDateTime = LocalDateTime.of(2021, 9, 9, 12, 0, 0);
        log.info("localDateTime is {},date is {}",localDateTime, Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant()));
        newOrderSingle.set(new TransactTime(localDateTime));
        newOrderSingle.set(new OrdType('1'));
        Session.sendToTarget(newOrderSingle,initiator.getSessions().get(0));
    }

    private static void testMarketDataRequest() throws SessionNotFound {
        MarketDataRequest marketDataRequest=new MarketDataRequest();
        marketDataRequest.setField(new SubscriptionRequestType('0'));
        marketDataRequest.setField(new MDReqID("TEST_marketDataRequest"));
        Session.sendToTarget(marketDataRequest,initiator.getSessions().get(0));
    }

}
