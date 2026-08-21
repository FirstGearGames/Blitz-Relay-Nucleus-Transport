#if UNITY_EDITOR || UNITY_2021_3_OR_NEWER
#define UNITY_ENGINE
#endif
#if UNITY_ENGINE && BLITZ_RELAY
using System.Net;
using System.Threading.Tasks;
using Nucleus.Integrations.BlitzRelay;
using Nucleus.Integrations.Newfarm;
using Nucleus.Managers.Client;
using Nucleus.Managers.Core;
using Nucleus.Transports;
using UnityEngine;

using Nucleus.Integrations.Unity.Managers.Transports;
namespace Nucleus.Integrations.Unity.Transports
{
    /// <summary>
    /// Inspector-configurable component for the Blitz Relay transport, which carries a session through a relay so peers reach
    /// each other without any of them being reachable. Place it on the UnityCoreManager's GameObject; the
    /// <see cref="UnityTransportManager"/> adds it to the CoreManager with these settings applied.
    /// </summary>
    /// <remarks>
    /// Compiled only when <c>BLITZ_RELAY</c> is defined, which is also what makes Unity load the assemblies this needs. Nothing
    /// relay-related is compiled or loaded in a project that has not asked for it.
    /// There is no port to listen on here and no address to dial. The peer that starts the server is given a room by the relay,
    /// and <see cref="RoomCode"/> is what every other peer needs. With <see cref="IsHostMigrationEnabled"/> set, nobody has to
    /// pass that around: the session registers with a directory, and a peer joins by <see cref="SessionId"/> alone, which keeps
    /// working after a handover when the room code no longer does.
    /// </remarks>
    /// <seealso cref="NewfarmHostMigration"/>
    [DisallowMultipleComponent]
    public class BlitzRelayTransport : NetworkTransport
    {
        /// <summary>
        /// The address of the relay carrying this session.
        /// </summary>
        [SerializeField]
        [Tooltip("Address of the Blitz Relay carrying this session.")]
        private string _relayAddress = "127.0.0.1";
        /// <summary>
        /// The port the relay listens on, defaulting to the port a Blitz Relay itself listens on unless it was told otherwise.
        /// </summary>
        [SerializeField]
        [Tooltip("Port the relay listens on. The default matches the relay's own.")]
        private ushort _relayPort = 7770;
        /// <summary>
        /// The key the relay admits peers with, which belongs to whoever runs the relay rather than to a player.
        /// </summary>
        [SerializeField]
        [Tooltip("Key the relay admits peers with. Set by whoever runs the relay, not by a player.")]
        private string _connectionKey = string.Empty;
        /// <summary>
        /// The room a joining peer asks for, left empty when this peer will host or when it joins by session instead.
        /// </summary>
        [SerializeField]
        [Tooltip("Room to join. Leave empty to host, or when joining by session id instead.")]
        private string _roomCode = string.Empty;
        /// <summary>
        /// How many clients a room this peer creates will hold.
        /// </summary>
        [SerializeField]
        [Tooltip("How many clients a room created by this peer will hold.")]
        private int _maximumClients = RelayTransport.DefaultMaximumClients;
        /// <summary>
        /// The largest datagram to build before the transport splits it.
        /// </summary>
        [SerializeField]
        [Tooltip("Largest datagram to build before it is split.")]
        private ushort _maximumTransmissionUnit = 1200;
        /// <summary>
        /// Whether the session registers with a directory so it survives losing whoever is hosting it.
        /// </summary>
        [SerializeField]
        [Tooltip("Register the session with a newfarm directory so it survives losing its host.")]
        private bool _isHostMigrationEnabled = true;
        /// <summary>
        /// The address of the directory the session registers with.
        /// </summary>
        [SerializeField]
        [Tooltip("Address of the newfarm directory the session registers with.")]
        private string _directoryAddress = "127.0.0.1";
        /// <summary>
        /// The port the directory listens on.
        /// </summary>
        [SerializeField]
        [Tooltip("Port the newfarm directory listens on.")]
        private ushort _directoryPort = 47778;

        /// <summary>
        /// The room this peer is hosting or has joined. Read it after starting a server: it is the only way another peer reaches
        /// this one, unless they are finding each other by session instead.
        /// </summary>
        public string RoomCode
        {
            get => _relayTransport is null ? _roomCode : _relayTransport.RoomCode;
            set => _roomCode = value;
        }

        /// <summary>
        /// Whether the session registers with a directory so it survives losing whoever is hosting it.
        /// </summary>
        public bool IsHostMigrationEnabled { get => _isHostMigrationEnabled; set => _isHostMigrationEnabled = value; }

        /// <summary>
        /// Keeps the session alive across the loss of its host, or <see langword="null"/> when
        /// <see cref="IsHostMigrationEnabled"/> is off or the transport has not been added yet.
        /// </summary>
        /// <remarks>
        /// Polled from this component's <c>Update</c>, so a game needs only to start hosting or to join through it. Everything
        /// else, being elected, adopting the world and finding where the session moved to, follows from that.
        /// Nothing about it is a relay. It drives a directory, and this transport reaches it through a
        /// <see cref="RelaySessionHost"/>, which is the only piece of a handover that knows what a room is.
        /// </remarks>
        public NewfarmHostMigration Migration { get; private set; }

        /// <summary>
        /// Names the session with the directory, which is what a host hands to its clients and the only thing a peer needs to
        /// find its way back after a handover.
        /// </summary>
        /// <seealso cref="Nucleus.Managers.Server.SessionDirectoryMessage"/>
        public ulong SessionId => Migration is null ? SessionDirectoryUnset : Migration.SessionId;

        /// <summary>
        /// The value <see cref="SessionId"/> reads when this peer holds no session.
        /// </summary>
        public const ulong SessionDirectoryUnset = 0;

        /// <summary>
        /// The transport this component configured, or <see langword="null"/> before it has.
        /// </summary>
        private RelayTransport _relayTransport;

        /// <inheritdoc/>
        public override async Task<Transport> AddToAsync(CoreManager coreManager)
        {
            _relayTransport = (RelayTransport)await coreManager.TransportManager.AddTransportAsync<RelayTransport>();

            _relayTransport.RelayEndPoint = new IPEndPoint(IPAddress.Parse(_relayAddress), _relayPort);
            _relayTransport.ConnectionKey = _connectionKey;
            _relayTransport.RoomCode = _roomCode;
            _relayTransport.MaximumClients = _maximumClients;
            _relayTransport.Configuration.MaximumTransmissionUnit = _maximumTransmissionUnit;

            if (_isHostMigrationEnabled)
            {
                /* Retention is the engine half of a handover and it is a client-side setting, not something the coordinator can
                 * reach in after the fact: the world is cleared or kept at the moment the link drops. A peer that opted into
                 * migration means to carry the world across exactly that moment, so opting in sets the mode here. */
                coreManager.ClientManager.DisconnectResetMode = DisconnectResetMode.RetainReceivedWorld;

                Migration = new NewfarmHostMigration(coreManager, new RelaySessionHost(_relayTransport), new IPEndPoint(IPAddress.Parse(_directoryAddress), _directoryPort));
            }

            return _relayTransport;
        }

        /// <summary>
        /// Opens a session, hosts it, and registers the room the relay names.
        /// </summary>
        /// <returns><see langword="true"/> when this peer is hosting the session.</returns>
        /// <remarks>Requires <see cref="IsHostMigrationEnabled"/>; without a directory there is no session to open.</remarks>
        public async Task<bool> StartHostingSessionAsync()
        {
            if (Migration is null)
            {
                Debug.LogError($"[{nameof(BlitzRelayTransport)}] Host migration is off, so there is no session to host. Start the server through the CoreManager instead.");

                return false;
            }

            return await Migration.StartHostingAsync();
        }

        /// <summary>
        /// Finds a session by its identifier and joins whatever room it currently lives in.
        /// </summary>
        /// <param name="sessionId">The session, as the host distributed it.</param>
        /// <returns><see langword="true"/> when this peer is in the room.</returns>
        public async Task<bool> JoinSessionAsync(ulong sessionId)
        {
            if (Migration is null)
            {
                Debug.LogError($"[{nameof(BlitzRelayTransport)}] Host migration is off, so a session cannot be joined. Set a room code and connect a client instead.");

                return false;
            }

            return await Migration.JoinAsync(sessionId);
        }

        /// <summary>
        /// Gives the session up to whoever the directory elects next, without leaving it and without stopping play.
        /// </summary>
        /// <returns><see langword="true"/> when this peer was hosting and has now given the session up.</returns>
        /// <remarks>
        /// The handover a game asks for on purpose: a host handing over from a menu or a hotkey rather than by quitting. This
        /// peer stops serving, the directory elects a successor at once instead of waiting out a heartbeat, and this peer rejoins
        /// as an ordinary client when the successor publishes where the session went.
        /// </remarks>
        public bool SurrenderHostingSession()
        {
            if (Migration is not null)
                return Migration.SurrenderHosting();

            Debug.LogError($"[{nameof(BlitzRelayTransport)}] Host migration is off, so there is no session to hand on. Stop the server through the CoreManager instead.");

            return false;
        }

        /// <summary>
        /// Drives the directory, which is what notices a lost host and acts on what the directory says to do about it.
        /// </summary>
        private void Update()
        {
            Migration?.Poll();
        }

        /// <summary>
        /// Releases the directory link with this component.
        /// </summary>
        private void OnDestroy()
        {
            Migration?.Dispose();
            Migration = null;
        }
    }
}

#endif
