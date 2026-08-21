# Blitz Relay Nucleus Transport

A Nucleus transport for Blitz Relay.

Nothing here listens, and there is no address to dial. Both sides of a session are ordinary clients of the relay, one of them holding the host role in a room, which is what lets peers reach each other when none of them is reachable and no port has been forwarded. What replaces an address is the **room code**: the peer that starts the server is given one by the relay, and every other peer needs that and nothing else.

```csharp
RelayTransport transport = (RelayTransport)await core.TransportManager.AddTransportAsync<RelayTransport>();

transport.RelayEndPoint = new IPEndPoint(relayAddress, 7770);
transport.ConnectionKey = connectionKey;

// Host: the relay names a room once the server connects. That name is what other peers need.
await transport.ConnectAsync(Invoker.Server);
string roomCode = transport.RoomCode;

// Client: the room code is the whole of what it takes to arrive.
transport.RoomCode = roomCode;
await transport.ConnectAsync(Invoker.Client);
```

## Surviving a lost host

A relayed room belongs to the peer that made it and dies with that peer, taking every other peer's link with it. So a handover is not a reconnection: it is a new room, with a name nobody could have known in advance, and no channel left between the survivors to tell them what it is.

`RelayHostMigration` answers that by registering the session with a [newfarm](https://github.com/FirstGearGames/Newfarm) directory. It notices the room dying, and either takes the session over (adopting the world the peer kept, opening a fresh room, publishing where it is) or waits to be told where the session went.

```csharp
RelayHostMigration migration = new(core, transport, directoryEndPoint);

core.ClientManager.DisconnectResetMode = DisconnectResetMode.RetainReceivedWorld;   // keep the world when the link drops

await migration.StartHostingAsync();        // host: opens a session, hosts it, publishes the room
await migration.JoinAsync(sessionId);       // client: finds the session wherever it now lives

migration.Poll();                           // once a frame
```

A peer joins by **session id** alone. It is never told a room code, and being told one would be worse than useless after a handover, since the room it names is the one that just died.

The session id is a bearer token: whoever holds it can be elected to host. Distribute it to your players and nobody else. Nucleus carries it for you through `SessionDirectoryMessage`.

## What it needs

Checked out beside this repository:

| | |
|---|---|
| [Nucleus](https://github.com/FirstGearGames/Nucleus) | the engine this is a transport for |
| SynapseSocket | carries this peer's datagrams to the relay |
| `BlitzRelay.Protocol` | the relay's message framing |
| [Newfarm](https://github.com/FirstGearGames/Newfarm) | only for `RelayHostMigration`; the transport itself does not use it |

The assembly must stay named `Nucleus.Integrations.BlitzRelay`. The engine grants it access to `CommonSocket`, whose connect, send and receive members are internal, and that grant is matched on assembly name.

## Notes

**It allocates nothing per message.** Every outbound message is framed into one buffer the link owns, and every inbound one is copied into an array rented from the shared pool, because the engine returns that array to that pool once it has read it. Measured at 0.00 bytes a message, both ways through the relay.

**A room needs a real size.** `ServerConfiguration.MaximumConnections` left unset means "no limit" on a transport that listens, and a relay refuses a room without a number, so `RelayTransport.MaximumClients` stands in when nothing else says one.

**Unity.** The transport targets `netstandard2.1`, and the Nucleus Unity integration carries an Inspector component for it behind the `BLITZ_RELAY` define.
