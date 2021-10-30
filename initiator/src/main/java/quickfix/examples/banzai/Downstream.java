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
import quickfix.fix50sp1.QuoteCancel;
import quickfix.fix50sp1.QuoteRequest;

import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Entry point for the Downstream application.
 */
public class Downstream {
    private static final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private static final Logger log = LoggerFactory.getLogger(Downstream.class);
    private static Downstream downstream;
    private boolean initiatorStarted = false;
    private static Initiator initiator = null;

    public Downstream(String[] args) throws Exception {
        InputStream inputStream = null;
        if (args.length == 0) {
            inputStream = Downstream.class.getResourceAsStream("banzai.cfg");
        } else if (args.length == 1) {
            inputStream = new FileInputStream(args[0]);
        }
        if (inputStream == null) {
            System.out.println("usage: " + Downstream.class.getName() + " [configFile].");
            return;
        }
        SessionSettings settings = new SessionSettings(inputStream);
        inputStream.close();

        boolean logHeartbeats = Boolean.valueOf(System.getProperty("logHeartbeats", "true"));

        OrderTableModel orderTableModel = new OrderTableModel();
        ExecutionTableModel executionTableModel = new ExecutionTableModel();
        BanzaiApplication application = new BanzaiApplication(orderTableModel, executionTableModel);
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

    public void stop() {
        shutdownLatch.countDown();
    }

    public static void main(String[] args) throws Exception {
        try {
            downstream = new Downstream(args);
            downstream.logon();
        } catch (Exception e) {
            log.info(e.getMessage(), e);
        }finally{
            testMarketDataRequest();
//            testNewOrderSingle();
//            for(int i=0;i<10;i++){
//                testQuoteRequest();
//            }
//            testQuoteCancel();
        }
        shutdownLatch.await();
    }

    private static void testNewOrderSingle() throws SessionNotFound {
        NewOrderSingle newOrderSingle = new NewOrderSingle();
        newOrderSingle.setField(new QuoteID("QuoteID_56ed394f-314c-4755-8a40-716e8e304113"));
        newOrderSingle.setField(new ClOrdID("ClOrdID_"+UUID.randomUUID().toString()));
        newOrderSingle.setField(new Account("usrid1000"));
        newOrderSingle.setField(new QuoteRespID("20122"));
        newOrderSingle.setField(new QuoteMsgID("GenIdeal"));
        newOrderSingle.setField(new TradeDate(new SimpleDateFormat("yyyyMMdd").format(new Date())));
        Session.sendToTarget(newOrderSingle,initiator.getSessions().get(0));
    }

    private static void testQuoteRequest() throws SessionNotFound{
        QuoteRequest qr=new QuoteRequest();
        qr.setField(new QuoteReqID("QuoteRequestID_"+ UUID.randomUUID().toString()));
        qr.setField(new Symbol("USDCNY"));
//        qr.setField(new ClOrdID("LimitOrderId"));
        qr.setField(new Side('1'));
        qr.setField(new QuoteType(0));
        qr.setField(new OrdType('2'));
        qr.setField(new OptPayAmount(Double.valueOf("1000")));
        qr.setField(new TransactTime(new Date().toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime()));
        Session.sendToTarget(qr,initiator.getSessions().get(0));
    }

    private static void testMarketDataRequest() throws SessionNotFound {
        MarketDataRequest marketDataRequest=new MarketDataRequest();
        marketDataRequest.setField(new SubscriptionRequestType('1'));
        marketDataRequest.setField(new MDReqID("TEST_marketDataRequest"));
        marketDataRequest.setField(new PartyID("PDP_PRICE"));
        marketDataRequest.setField(new Symbol("CNY.JPY"));
        marketDataRequest.setField(new MarketDepth(1));
        Session.sendToTarget(marketDataRequest,initiator.getSessions().get(0));
    }

    private static void testQuoteCancel() throws SessionNotFound {
        QuoteCancel quoteCancel=new QuoteCancel();
        quoteCancel.setField(new QuoteID("TEST_QuoteID"));
        quoteCancel.setField(new QuoteCancelType(5));
        quoteCancel.setField(new QuoteReqID("test_QuoteReqID"));
        Session.sendToTarget(quoteCancel,initiator.getSessions().get(0));
    }

}
