#if UNITY_EDITOR && BLITZ_RELAY
using Nucleus.Integrations.Unity.Transports;
using UnityEditor;
using UnityEngine;

namespace Nucleus.Integrations.Unity.Editor
{
    /// <summary>
    /// Inspector presentation for <see cref="BlitzRelayTransport"/>: the relay that carries the session, the room, and the
    /// directory that lets the session survive losing its host. All presentation lives here per project convention, the
    /// MonoBehaviour carrying no layout attributes.
    /// </summary>
    [CustomEditor(typeof(BlitzRelayTransport))]
    public class BlitzRelayTransportEditor : UnityEditor.Editor
    {
        /// <inheritdoc/>
        public override void OnInspectorGUI()
        {
            serializedObject.Update();

            EditorGUILayout.HelpBox("Peers reach each other through a relay, so none of them has to be reachable and there is no port to forward. Nothing here listens: the peer that starts the server is given a room by the relay, and every other peer needs that room's code.\n\nWith host migration on, nobody has to pass a room code around. The session registers with a directory and a peer joins by session id, which is the only thing that still works after a handover, since the room dies with whoever was hosting it.", MessageType.Info);

            EditorGUILayout.Space();
            EditorGUILayout.LabelField("Relay", EditorStyles.boldLabel);
            EditorGUILayout.PropertyField(serializedObject.FindProperty("_relayAddress"), new GUIContent("Address", "Address of the Blitz Relay carrying this session."));
            EditorGUILayout.PropertyField(serializedObject.FindProperty("_relayPort"), new GUIContent("Port", "Port the relay listens on."));
            EditorGUILayout.PropertyField(serializedObject.FindProperty("_connectionKey"), new GUIContent("Connection Key", "Key the relay admits peers with. Belongs to whoever runs the relay, not to a player."));

            EditorGUILayout.Space();
            EditorGUILayout.LabelField("Room", EditorStyles.boldLabel);
            EditorGUILayout.PropertyField(serializedObject.FindProperty("_roomCode"), new GUIContent("Room Code", "Room to join. Leave empty to host, or when joining by session id instead."));
            EditorGUILayout.PropertyField(serializedObject.FindProperty("_maximumClients"), new GUIContent("Maximum Clients", "How many clients a room created by this peer will hold. A relay refuses a room without a real number, so this cannot mean unlimited."));
            EditorGUILayout.PropertyField(serializedObject.FindProperty("_maximumTransmissionUnit"), new GUIContent("Transmission Unit", "Largest datagram to build before it is split."));

            EditorGUILayout.Space();
            EditorGUILayout.LabelField("Surviving a lost host", EditorStyles.boldLabel);

            SerializedProperty isHostMigrationEnabledProperty = serializedObject.FindProperty("_isHostMigrationEnabled");

            EditorGUILayout.PropertyField(isHostMigrationEnabledProperty, new GUIContent("Host Migration", "Register the session with a newfarm directory so it survives losing whoever is hosting it."));

            using (new EditorGUI.DisabledScope(!isHostMigrationEnabledProperty.boolValue))
            {
                EditorGUILayout.PropertyField(serializedObject.FindProperty("_directoryAddress"), new GUIContent("Directory Address", "Address of the newfarm directory the session registers with."));
                EditorGUILayout.PropertyField(serializedObject.FindProperty("_directoryPort"), new GUIContent("Directory Port", "Port the newfarm directory listens on."));
            }

            if (isHostMigrationEnabledProperty.boolValue)
                EditorGUILayout.HelpBox("Clients keep the world when their link drops only if ClientManager.DisconnectResetMode is set to RetainReceivedWorld. Without it a survivor is elected to host an empty world.", MessageType.Warning);

            EditorGUILayout.Space();
            EditorGUILayout.LabelField("Live", EditorStyles.boldLabel);

            BlitzRelayTransport blitzRelayTransport = (BlitzRelayTransport)target;

            using (new EditorGUI.DisabledScope(true))
            {
                EditorGUILayout.TextField(new GUIContent("Current Room", "The room this peer is hosting or has joined, as the relay named it."), Application.isPlaying ? blitzRelayTransport.RoomCode : string.Empty);
                EditorGUILayout.TextField(new GUIContent("Session Id", "What a host hands to its clients, and the only thing a peer needs to find its way back after a handover."), Application.isPlaying && blitzRelayTransport.SessionId != BlitzRelayTransport.SessionDirectoryUnset ? blitzRelayTransport.SessionId.ToString("x16") : string.Empty);
            }

            serializedObject.ApplyModifiedProperties();
        }
    }
}

#endif
