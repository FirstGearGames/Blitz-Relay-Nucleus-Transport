using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using BlitzRelay.Protocol;
using CodeBoost.Logging;
using Nucleus.Integrations.BlitzRelay;
using Nucleus.Transports;
using SynapseSocket.Core;
using SynapseSocket.Core.Configuration;
using Xunit;

namespace Nucleus.Integrations.BlitzRelay.Tests;

/// <summary>
/// Drives a real <see cref="RelayLink"/> against a plain <see cref="SynapseManager"/> standing in for the relay, over loopback, to
/// prove the link keeps no connection once the socket has released it.
/// </summary>
/// <remarks>
/// The stand-in never answers the handshake, which is fine: nothing here needs the link to be ready, only connected and then ended.
/// A send is how each test looks for a kept connection. One that still reaches the socket either logs the socket refusing a dead
/// connection, or, once the socket reuses the object for another session, arrives at whoever holds that session.
/// </remarks>
public class RelayLinkTests
{
    /// <summary>
    /// How long a wait for an expected outcome runs before it is treated as a failure.
    /// </summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// How long a wait for an outcome that must not happen runs before it is taken as not happening.
    /// </summary>
    private static readonly TimeSpan NegativeWaitTimeout = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Longer than the socket's one second window in which a repeated handshake counts as an answer rather than a reconnect.
    /// </summary>
    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromMilliseconds(1200);

    /// <summary>
    /// What the link logs when a send reaches the socket and the socket refuses it.
    /// </summary>
    private const string SendFailedMessage = "A send to the relay failed";

    /// <summary>
    /// Once the relay closes the link, a send fails quietly rather than reaching the socket with the released connection.
    /// </summary>
    [Fact]
    public void RelayClosingTheLink_LeavesNoConnectionForASendToReach()
    {
        using SynapseManager relay = CreateStartedRelay(port: 0);

        RelayLink relayLink = RelayLink.CreateClient("key", "ROOM");
        bool isClosed = false;
        relayLink.Closed += () => isClosed = true;

        try
        {
            ConnectAndWaitForAuthentication(relayLink, relay);

            relay.Disconnect(relay.Connections.Connections[0]);
            Assert.True(PumpUntil(() => isClosed, WaitTimeout, relayLink, relay), "The link never saw the relay close it.");

            AssertSendReachesNothing(relayLink, relay);
        }
        finally
        {
            relayLink.Dispose();
        }
    }

    /// <summary>
    /// When the relay's endpoint handshakes again, the socket closes and releases the link's session and hands the same connection
    /// object to the new one. The link has let go by then, so its send neither logs nor arrives at the newcomer.
    /// </summary>
    [Fact]
    public void RelayEndPointReconnecting_LeavesNoConnectionForASendToReach()
    {
        SynapseManager relay = CreateStartedRelay(port: 0);
        int relayPort = relay.BoundEndPoints[0].Port;

        RelayLink relayLink = RelayLink.CreateClient("key", "ROOM");
        bool isClosed = false;
        relayLink.Closed += () => isClosed = true;

        SynapseManager? newcomer = null;

        try
        {
            ConnectAndWaitForAuthentication(relayLink, relay);
            IPEndPoint linkEndPoint = relay.Connections.Connections[0].RemoteEndPoint;

            // Disposed silently, so the link hears no goodbye and still holds the session when the same endpoint handshakes again.
            relay.Dispose();
            PumpUntil(() => false, ReconnectDelay, relayLink);

            /* Only the close is waited on. The link sends the new session away, since it did not open it, so the newcomer's
             * connection may already be released, and so no longer the newcomer's to read, by the time the close is seen. */
            newcomer = CreateStartedRelay(relayPort);
            newcomer.Connect(new(IPAddress.Loopback, linkEndPoint.Port));
            Assert.True(PumpUntil(() => isClosed, WaitTimeout, relayLink, newcomer), "The reconnect never replaced the link's session.");

            AssertSendReachesNothing(relayLink, newcomer);
        }
        finally
        {
            relayLink.Dispose();
            relay.Dispose();
            newcomer?.Dispose();
        }
    }

    /// <summary>
    /// Disposing the link still says goodbye to the relay, and a send afterwards finds no connection to reach.
    /// </summary>
    [Fact]
    public void DisposingTheLink_SaysGoodbyeAndLeavesNoConnectionForASendToReach()
    {
        using SynapseManager relay = CreateStartedRelay(port: 0);

        RelayLink relayLink = RelayLink.CreateClient("key", "ROOM");

        try
        {
            ConnectAndWaitForAuthentication(relayLink, relay);

            relayLink.Dispose();
            Assert.True(PumpUntil(() => relay.Connections.Count == 0, WaitTimeout, relayLink, relay), "The relay never heard the link's goodbye.");

            AssertSendReachesNothing(relayLink, relay);
        }
        finally
        {
            relayLink.Dispose();
        }
    }

    /// <summary>
    /// A stranger that handshakes into the link's socket before the relay has answered is not taken for the relay, so it is sent
    /// nothing, the connection key least of all.
    /// </summary>
    /// <remarks>
    /// The link used to adopt whichever connection established first. A stranger that won that race became the link, was sent the
    /// authentication with the connection key in it, and was sent everything the link sent afterwards.
    /// </remarks>
    [Fact]
    public void StrangerReachingTheLinkBeforeTheRelay_IsNotTakenForTheRelay()
    {
        using UdpClient silentRelay = new(new IPEndPoint(IPAddress.Loopback, 0));
        using SynapseManager stranger = CreateStartedRelay(port: 0);

        int strangerReceivedCount = 0;
        stranger.PacketReceived += _ => strangerReceivedCount++;

        RelayLink relayLink = RelayLink.CreateClient("key", "ROOM");

        try
        {
            IPEndPoint linkEndPoint = ConnectToSilentRelay(relayLink, silentRelay);

            stranger.Connect(linkEndPoint);
            Assert.False(PumpUntil(() => strangerReceivedCount > 0, NegativeWaitTimeout, relayLink, stranger), "The link took a stranger for the relay and sent it the handshake.");
        }
        finally
        {
            relayLink.Dispose();
        }
    }

    /// <summary>
    /// A stranger that handshakes into the link's socket cannot make the link ready by sending what the relay sends on a
    /// successful join.
    /// </summary>
    /// <remarks>
    /// The link read every message its socket delivered as the relay's, whichever connection it arrived on, so a stranger could
    /// report a join the relay never granted.
    /// </remarks>
    [Fact]
    public void StrangersJoinSuccess_DoesNotReadyTheLink()
    {
        using UdpClient silentRelay = new(new IPEndPoint(IPAddress.Loopback, 0));
        using SynapseManager stranger = CreateStartedRelay(port: 0);

        byte[] joinSuccess = MessageCodec.CreateJoinSuccess();

        // Sent the moment the stranger's session is up, before anything the link does in answer can reach it.
        stranger.ConnectionEstablished += connectionEventArgs => stranger.Send(connectionEventArgs.Connection, new(joinSuccess), isReliable: true);

        RelayLink relayLink = RelayLink.CreateClient("key", "ROOM");
        bool isReady = false;
        relayLink.Ready += () => isReady = true;

        try
        {
            IPEndPoint linkEndPoint = ConnectToSilentRelay(relayLink, silentRelay);

            stranger.Connect(linkEndPoint);
            Assert.False(PumpUntil(() => isReady, NegativeWaitTimeout, relayLink, stranger), "A stranger's message made the link ready.");
            Assert.False(relayLink.IsReady);
        }
        finally
        {
            relayLink.Dispose();
        }
    }

    /// <summary>
    /// Connects the link to a relay address that reads the link's handshake and never answers it, and returns the endpoint the
    /// link sends from.
    /// </summary>
    /// <param name="relayLink">The link under test.</param>
    /// <param name="silentRelay">The bare socket standing at the relay's address.</param>
    /// <returns>The link's own endpoint, as the relay sees it.</returns>
    /// <remarks>
    /// A relay that answered would be established before a stranger could arrive. The bare socket learns the link's port from its
    /// handshake exactly as a relay would, which is all a stranger needs.
    /// </remarks>
    private static IPEndPoint ConnectToSilentRelay(RelayLink relayLink, UdpClient silentRelay)
    {
        Assert.True(relayLink.TryConnect((IPEndPoint)silentRelay.Client.LocalEndPoint!, maximumTransmissionUnit: 1200));

        silentRelay.Client.ReceiveTimeout = (int)WaitTimeout.TotalMilliseconds;

        IPEndPoint? linkEndPoint = null;
        silentRelay.Receive(ref linkEndPoint);

        return linkEndPoint!;
    }

    /// <summary>
    /// Creates and starts an engine standing in for the relay, bound to loopback.
    /// </summary>
    /// <param name="port">The port to bind, or zero for any.</param>
    /// <returns>The started engine.</returns>
    private static SynapseManager CreateStartedRelay(int port)
    {
        SynapseManager relay = new(new SynapseConfig { BindEndPoints = { new(IPAddress.Loopback, port) } });
        relay.Start();

        return relay;
    }

    /// <summary>
    /// Connects the link to the stand-in and waits for its authentication to arrive, which the link sends the moment it holds its
    /// established connection.
    /// </summary>
    /// <param name="relayLink">The link under test.</param>
    /// <param name="relay">The stand-in relay.</param>
    private static void ConnectAndWaitForAuthentication(RelayLink relayLink, SynapseManager relay)
    {
        int receivedCount = 0;
        relay.PacketReceived += _ => receivedCount++;

        Assert.True(relayLink.TryConnect(new(IPAddress.Loopback, relay.BoundEndPoints[0].Port), maximumTransmissionUnit: 1200));
        Assert.True(PumpUntil(() => receivedCount > 0, WaitTimeout, relayLink, relay), "The link never reached the relay.");
    }

    /// <summary>
    /// Asserts a send from the link is refused without reaching the socket, and that nothing arrives at the given engine.
    /// </summary>
    /// <param name="relayLink">The link under test.</param>
    /// <param name="receiver">The engine a kept connection would deliver to.</param>
    private static void AssertSendReachesNothing(RelayLink relayLink, SynapseManager receiver)
    {
        int receivedCount = 0;
        receiver.PacketReceived += _ => receivedCount++;

        // Registered only now, so the assertion reads what the send logs and nothing the connect did.
        CapturingLogger capturingLogger = new();

        Assert.False(relayLink.TrySendAsClient(Channel.Reliable, new(new byte[] { 1, 2, 3 })));
        Assert.False(capturingLogger.HasErrorContaining(SendFailedMessage), "The send reached the socket with a released connection.");
        Assert.False(PumpUntil(() => receivedCount > 0, NegativeWaitTimeout, relayLink, receiver), "The send arrived over a released connection.");
    }

    /// <summary>
    /// Polls the link and every given engine until a condition holds or <paramref name="timeout"/> passes.
    /// </summary>
    /// <param name="condition">The outcome waited for.</param>
    /// <param name="timeout">How long to wait before giving up.</param>
    /// <param name="relayLink">The link under test.</param>
    /// <param name="synapseManagers">The stand-in relay, and any other engine the test drives.</param>
    /// <returns>True when the condition held in time.</returns>
    private static bool PumpUntil(Func<bool> condition, TimeSpan timeout, RelayLink relayLink, params SynapseManager[] synapseManagers)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();

        while (stopwatch.Elapsed < timeout)
        {
            foreach (SynapseManager synapseManager in synapseManagers)
                synapseManager.Poll();

            relayLink.Poll();

            if (condition())
                return true;

            Thread.Sleep(5);
        }

        return false;
    }

    /// <summary>
    /// Records every error logged while it is the process logger.
    /// </summary>
    private sealed class CapturingLogger : ILogger
    {
        /// <summary>
        /// Every error logged, in arrival order.
        /// </summary>
        private readonly ConcurrentQueue<string> _errors = new();

        /// <summary>
        /// Creates the logger and registers it as the process logger.
        /// </summary>
        public CapturingLogger()
        {
            LoggingService.UseLogger(this);
        }

        /// <inheritdoc/>
        public LoggerSetting GetLoggerSetting() => LoggerSetting.LoggerServiceSetting;

        /// <inheritdoc/>
        public bool DisableUnconditionalDevelopmentStacktrace() => true;

        /// <inheritdoc/>
        public void LogInformation(string message) { }

        /// <inheritdoc/>
        public void LogWarning(string message) { }

        /// <inheritdoc/>
        public void LogError(string message) => _errors.Enqueue(message);

        /// <summary>
        /// Returns whether any logged error contains the given text.
        /// </summary>
        /// <param name="text">The text to look for.</param>
        /// <returns>True when at least one logged error contains it.</returns>
        public bool HasErrorContaining(string text) => _errors.Any(error => error.Contains(text));
    }
}
