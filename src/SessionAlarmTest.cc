/* Copyright (c) 2011-2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "TestUtil.h"
#include "MembershipClient.h"
#include "RpcWrapper.h"
#include "SessionAlarm.h"
#include "Transport.h"

namespace RAMCloud {

// The following class is used for testing.
class AlarmSession : public Transport::Session {
  public:
    Context& context;
    explicit AlarmSession(Context& context)
        : context(context), notifiers(), alarm(NULL)
    {
        setServiceLocator("test:alarm");
    }

    class ClientRpc: public Transport::ClientRpc {
      public:
        ClientRpc(Context& context, Buffer* request, Buffer* response)
            : Transport::ClientRpc(context, request, response) {}
        virtual void cancelCleanup()
        {
            appendLog("cancelCleanup");
        }
    };

    virtual Transport::ClientRpc*
    clientSend(Buffer* request, Buffer* response)
    {
        assert(false);
    }

    // Must implement sendRequest to handle poll RPCs generated by
    // SessionAlarmTimer.
    virtual void
    sendRequest(Buffer* request, Buffer* response,
            Transport::RpcNotifier* notifier)
    {
        notifiers.push_back(notifier);
        if (alarm != NULL)
            alarm->rpcStarted();
        appendLog(format("sendRequest: opcode %s",
                WireFormat::opcodeSymbol(*request)));
    }

    void finishRpcs()
    {
        while (notifiers.size() > 0) {
            notifiers.back()->failed();
            notifiers.pop_back();
            if (alarm != NULL)
                alarm->rpcFinished();
        }
    }

    // Must respond to abort calls generated by SessionAlarmTimer.
    virtual void
    abort(const string& message)
    {
        appendLog(format("abort: %s", message.c_str()));
        finishRpcs();
    }

    virtual void cancelRequest(Transport::RpcNotifier* notifier)
    {
        appendLog("cancel");
        for (uint32_t i = 0; i < notifiers.size(); i++) {
            if (notifiers[i] == notifier) {
                notifiers[i] = notifiers.back();
                notifiers.pop_back();
                if (alarm != NULL)
                    alarm->rpcFinished();
                return;
            }
        }
    }

    static void appendLog(string message)
    {
        if (log.length() != 0) {
            log.append(", ");
        }
        log.append(message);
    }

    virtual void release() {}
    std::vector<Transport::RpcNotifier*> notifiers;
    SessionAlarm* alarm;
    static string log;
  private:
    DISALLOW_COPY_AND_ASSIGN(AlarmSession);
};
std::string AlarmSession::log;

class SessionAlarmTest : public ::testing::Test {
  public:
    Context context;
    SessionAlarmTimer timer;
    AlarmSession* session;
    Transport::SessionRef sessionRef;

    SessionAlarmTest()
        : context()
        , timer(context)
        , session()
        , sessionRef()
    {
        session = new AlarmSession(context);
        sessionRef = session;
        AlarmSession::log.clear();
    }

    ~SessionAlarmTest()
    {
        Cycles::mockTscValue = 0;
    }
  private:
    DISALLOW_COPY_AND_ASSIGN(SessionAlarmTest);
};

class SessionAlarmTimerTest : public SessionAlarmTest {
};

TEST_F(SessionAlarmTest, basics) {
    // Run a test that should produce a timeout several times, and make
    // sure that at least once a timeout occurs in a period that we'd
    // expect (scheduling glitches on the machine could cause timeouts
    // to occasionally take longer than this).
    TestLog::Enable _;
    SessionAlarm alarm(timer, *session, 0);
    session->alarm = &alarm;
    double elapsed = 0.0;
    double desired = .035;
    for (int i = 0; i < 10; i++) {
        AlarmSession::log.clear();
        GetServerIdRpc rpc(context, sessionRef);
        uint64_t start = Cycles::rdtsc();
        while (!rpc.isReady()) {
            elapsed = Cycles::toSeconds(Cycles::rdtsc() - start);
            if (elapsed > desired) {
                break;
            }
            context.dispatch->poll();
        }
        if (elapsed < desired) {
            EXPECT_EQ("sendRequest: opcode GET_SERVER_ID, sendRequest: "
                    "opcode PING, abort: server at test:alarm is not "
                    "responding", AlarmSession::log);
            EXPECT_STREQ("FAILED", rpc.stateString());
            break;
        }
    }
    EXPECT_GT(desired, elapsed);
}

TEST_F(SessionAlarmTest, constructor_timeoutTooShort) {
    SessionAlarm alarm(timer, *session, 10);
    EXPECT_EQ(15, alarm.pingMs);
    EXPECT_EQ(30, alarm.abortMs);
}

TEST_F(SessionAlarmTest, destructor_cleanupOutstandingRpcs) {
    Tub<SessionAlarm> alarm;
    alarm.construct(timer, *session, 0);
    alarm->rpcStarted();
    alarm->rpcStarted();
    alarm.destroy();
    EXPECT_EQ(0U, timer.activeAlarms.size());
}

TEST_F(SessionAlarmTest, rpcStarted) {
    SessionAlarm alarm(timer, *session, 0);

    // First RPC starts: must start timer.
    alarm.rpcStarted();
    EXPECT_EQ(1, alarm.outstandingRpcs);
    EXPECT_EQ(1U, timer.activeAlarms.size());
    EXPECT_TRUE(timer.isRunning());

    // Additional RPC starts: only the SessionAlarm changes.
    alarm.rpcStarted();
    EXPECT_EQ(2, alarm.outstandingRpcs);
    EXPECT_EQ(1U, timer.activeAlarms.size());
    EXPECT_TRUE(timer.isRunning());

    // Start another RPC on a different alarm.
    SessionAlarm alarm2(timer, *session, 0);
    alarm2.rpcStarted();
    EXPECT_EQ(1, alarm2.outstandingRpcs);
    EXPECT_EQ(2U, timer.activeAlarms.size());
}

TEST_F(SessionAlarmTest, rpcFinished_removeFromActiveAlarms) {
    SessionAlarm alarm1(timer, *session, 0);
    alarm1.rpcStarted();
    SessionAlarm alarm2(timer, *session, 0);
    alarm2.rpcStarted();
    alarm2.rpcStarted();
    alarm2.waitingForResponseMs = 10;
    SessionAlarm alarm3(timer, *session, 0);
    alarm3.rpcStarted();

    alarm2.rpcFinished();
    EXPECT_EQ(0, alarm2.waitingForResponseMs);
    EXPECT_EQ(1, alarm2.outstandingRpcs);
    EXPECT_EQ(3U, timer.activeAlarms.size());
    alarm2.rpcFinished();
    EXPECT_EQ(0, alarm2.outstandingRpcs);
    EXPECT_EQ(2U, timer.activeAlarms.size());
    EXPECT_EQ(&alarm1, timer.activeAlarms[0]);
    EXPECT_EQ(&alarm3, timer.activeAlarms[1]);

    alarm1.rpcFinished();
    EXPECT_EQ(1U, timer.activeAlarms.size());
    EXPECT_EQ(&alarm3, timer.activeAlarms[0]);
    alarm3.rpcFinished();
    EXPECT_EQ(0U, timer.activeAlarms.size());
}

TEST_F(SessionAlarmTimerTest, destructor) {
    Tub<SessionAlarmTimer> timer2;
    timer2.construct(context);
    SessionAlarm alarm1(*timer2, *session, 0);
    alarm1.rpcStarted();
    alarm1.rpcStarted();
    SessionAlarm alarm2(*timer2, *session, 0);
    alarm2.rpcStarted();

    // Wait until ping RPCs get sent on each alarm.
    for (int i = 0; (i < 10) && (timer2->pings.size() < 2); i++) {
        timer2->handleTimerEvent();
    }
    EXPECT_EQ(2U, timer2->pings.size());
    AlarmSession::log.clear();

    timer2.destroy();
    EXPECT_EQ(0, alarm1.outstandingRpcs);
    EXPECT_EQ(0, alarm2.outstandingRpcs);
    EXPECT_EQ("cancel, cancel", AlarmSession::log);
}

TEST_F(SessionAlarmTimerTest, handleTimerEvent_incrementResponseTime) {
    SessionAlarm alarm1(timer, *session, 0);
    SessionAlarm alarm2(timer, *session, 0);
    alarm1.rpcStarted();
    alarm2.rpcStarted();
    alarm2.waitingForResponseMs = 5;

    timer.handleTimerEvent();
    EXPECT_EQ(5, alarm1.waitingForResponseMs);
    EXPECT_EQ(10, alarm2.waitingForResponseMs);
}

TEST_F(SessionAlarmTimerTest, handleTimerEvent_pingAndAbortSession) {
    TestLog::Enable _;
    SessionAlarm alarm1(timer, *session, 0);
    alarm1.rpcStarted();
    alarm1.waitingForResponseMs = 25;

    timer.handleTimerEvent();
    EXPECT_EQ("sendRequest: opcode PING", AlarmSession::log);
    AlarmSession::log.clear();
    EXPECT_EQ("handleTimerEvent: initiated ping request to test:alarm",
            TestLog::get());
    timer.handleTimerEvent();
    EXPECT_EQ("abort: server at test:alarm is not responding",
            AlarmSession::log);
}

TEST_F(SessionAlarmTest, handleTimerEvent_cleanupPings) {
    TestLog::Enable _;
    // Create 3 different alarms.
    SessionAlarm alarm1(timer, *session, 0);
    session->alarm = &alarm1;
    alarm1.rpcStarted();

    AlarmSession* session2 = new AlarmSession(context);
    Transport::SessionRef ref2 = session2;
    SessionAlarm alarm2(timer, *session2, 0);
    session2->alarm = &alarm2;
    alarm2.rpcStarted();

    AlarmSession* session3 = new AlarmSession(context);
    Transport::SessionRef ref3 = session3;
    SessionAlarm alarm3(timer, *session3, 0);
    session3->alarm = &alarm3;
    alarm3.rpcStarted();

    // Wait until ping RPCs get sent on all three.
    for (int i = 0; (i < 10) && (timer.pings.size() < 3); i++) {
        timer.handleTimerEvent();
    }
    EXPECT_EQ(3U, timer.pings.size());
    EXPECT_EQ("handleTimerEvent: initiated ping request to test:alarm | "
            "handleTimerEvent: initiated ping request to test:alarm | "
            "handleTimerEvent: initiated ping request to test:alarm",
            TestLog::get());
    TestLog::reset();

    // Finish 2 of the pings (and check for proper cleanup).
    session->finishRpcs();
    session2->finishRpcs();
    timer.handleTimerEvent();
    EXPECT_EQ(1U, timer.pings.size());
    EXPECT_EQ(&alarm3, timer.pings.begin()->first);

    // Finish the third pings and check for cleanup.
    session3->finishRpcs();
    timer.handleTimerEvent();
    EXPECT_EQ(0U, timer.pings.size());
}

TEST_F(SessionAlarmTimerTest, handleTimerEvent_restartTimer) {
    TestLog::Enable _;
    Cycles::mockTscValue = 1000;
    timer.timerIntervalTicks = 100;
    context.dispatch->poll();
    SessionAlarm alarm1(timer, *session, 0);
    alarm1.rpcStarted();
    Cycles::mockTscValue = 2000;
    context.dispatch->poll();
    EXPECT_TRUE(timer.isRunning());
    EXPECT_EQ(5, alarm1.waitingForResponseMs);
    alarm1.rpcFinished();
    Cycles::mockTscValue = 3000;
    context.dispatch->poll();
    EXPECT_FALSE(timer.isRunning());
}

}  // namespace RAMCloud
