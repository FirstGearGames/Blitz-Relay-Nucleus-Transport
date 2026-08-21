# Blitz Relay Nucleus Transport

A Nucleus transport for Blitz Relay, with a provided Unity integration.

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

None of that is the relay's problem to solve, and none of it is solved here. It is solved by a [newfarm](https://github.com/FirstGearGames/Newfarm) directory and by `Nucleus.Integrations.Newfarm.NewfarmHostMigration`, which holds the session, notices the host going, and either takes the session over (adopting the world the peer kept, standing the session up again, publishing where it now is) or waits to be told where the session went. Every word of that is the same whether a session is carried by a relay, an allocation service, or a host and port.

What this repository contributes is `RelaySessionHost`, the `ISessionHost` the coordinator drives: it opens a room, names it, joins somebody else's, and leaves. Its adapter tag is `"blitzrelay"`, which is how a peer receiving a credential knows it is a room code rather than some other service's idea of an address.

```csharp
NewfarmHostMigration migration = new(core, new RelaySessionHost(transport), directoryEndPoint);

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
| `Nucleus.Integrations.Newfarm` | in the Nucleus repository, and only for `RelaySessionHost`; the transport itself does not use it |

Newfarm itself is not referenced from here at all. Nothing in this assembly talks to a directory, and nothing in it knows what a session identity or an epoch is; that arrives behind `Nucleus.Integrations.Newfarm`, which is where [newfarm](https://github.com/FirstGearGames/Newfarm) is a dependency.

The assembly must stay named `Nucleus.Integrations.BlitzRelay`. The engine grants it access to `CommonSocket`, whose connect, send and receive members are internal, and that grant is matched on assembly name. The coordinator needs no such grant: it drives an ordinary `Transport` through `ISessionHost` and touches nothing internal.

## Notes

**It allocates nothing per message.** Every outbound message is framed into one buffer the link owns, and every inbound one is copied into an array rented from the shared pool, because the engine returns that array to that pool once it has read it. Measured at 0.00 bytes a message, both ways through the relay.

**A room needs a real size.** `ServerConfiguration.MaximumConnections` left unset means "no limit" on a transport that listens, and a relay refuses a room without a number, so `RelayTransport.MaximumClients` stands in when nothing else says one.

**`RoomCode` answers two questions, `HostedRoomCode` answers one.** `RoomCode` is the room this peer hosts if it has one and otherwise the room it was told to join, which is the convenient reading for a game driving the transport directly. `HostedRoomCode` is strictly this peer's own and empty when its hosting side is down, which is the only honest answer to "what should be published to a directory" and to "prove you are still hosting".

## Layout

| | |
|---|---|
| `Nucleus.Integrations.BlitzRelay` | The transport. Plain C#, `netstandard2.1`, no game engine anywhere in it. |
| `Nucleus.Integrations.BlitzRelay/Unity` | Inspector components for Unity, and nothing else. Excluded from the build above. |

The split is the same one Nucleus itself makes. The transport knows nothing about Unity and is driven entirely in code, exactly as `Synapse` is; the `Unity` folder adds a `MonoBehaviour` that configures it from the Inspector and drives what needs a frame, in the manner of `UnitySystemManager` and its siblings. The transport project removes `Unity\**` from its compile items, so the engine-agnostic assembly cannot pick it up by accident.

Take the `Unity` folder only if you want the components. Its scripts belong in the same assembly as the Nucleus Unity integration, because `BlitzRelayTransport` derives from that integration's `NetworkTransport` and is added to the CoreManager by its `UnityTransportManager`. Copy or junction it into your project beside `Nucleus.Integrations.Unity`, and define `BLITZ_RELAY` so it compiles.
