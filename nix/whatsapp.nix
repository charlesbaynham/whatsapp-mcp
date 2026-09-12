# WhatsApp bridge and its local clients. The Go bridge holds the WhatsApp
# session and owns the store; every client (the MCP server, the Hindsight
# forwarder, ...) talks to it over a Unix socket and nothing else. Opening
# that socket is the only credential, so each client is its own system user
# admitted by membership of the socket group and sandboxed away from the
# store and from the network it doesn't need.
{ config, lib, pkgs, ... }:

let
  cfg = config.services.whatsapp;
  storeDir = "${cfg.stateDir}/store";
  socketPath = "/run/whatsapp/bridge.sock";
  clientsGroup = "whatsapp-clients";

  hardening = {
    NoNewPrivileges = true;
    PrivateTmp = true;
    PrivateDevices = true;
    ProtectSystem = "strict";
    ProtectHome = true;
    ProtectKernelTunables = true;
    ProtectKernelModules = true;
    ProtectKernelLogs = true;
    ProtectControlGroups = true;
    ProtectClock = true;
    ProtectProc = "invisible";
    ProcSubset = "pid";
    RestrictSUIDSGID = true;
    RestrictRealtime = true;
    LockPersonality = true;
    RestrictNamespaces = true;
    SystemCallFilter = [ "@system-service" "~@privileged" "~@resources" ];
    SystemCallArchitectures = "native";
    CapabilityBoundingSet = "";
    AmbientCapabilities = "";
    UMask = "0077";
  };

  # A client unit: its own user, socket-group membership, no store access,
  # and only the address families it declares (AF_UNIX for the bridge socket
  # is always included).
  mkClientService = name: client: {
    description = client.description;
    wantedBy = [ "multi-user.target" ];
    after = [ "network-online.target" "whatsapp-bridge.service" ];
    wants = [ "network-online.target" ];
    requires = [ "whatsapp-bridge.service" ];
    path = client.path;
    environment = { WHATSAPP_BRIDGE_URL = "unix:${socketPath}"; } // client.environment;
    serviceConfig = hardening // {
      User = client.user;
      Group = client.user;
      SupplementaryGroups = [ clientsGroup ];
      RestrictAddressFamilies = [ "AF_UNIX" ] ++ client.addressFamilies;
      WorkingDirectory = "/";
      ExecStart = client.execStart;
      Restart = "always";
      RestartSec = 5;
    } // lib.optionalAttrs client.needsState {
      StateDirectory = "whatsapp-${name}";
    } // lib.optionalAttrs (client.environmentFile != null) {
      EnvironmentFile = "-${client.environmentFile}";
    };
  };

  clientOpts = { name, ... }: {
    options = {
      description = lib.mkOption { type = lib.types.str; default = "WhatsApp client: ${name}"; };
      user = lib.mkOption {
        type = lib.types.str;
        default = "whatsapp-${name}";
        description = "System user the client runs as. Created automatically.";
      };
      execStart = lib.mkOption { type = lib.types.str; description = "The command to run."; };
      environment = lib.mkOption { type = lib.types.attrsOf lib.types.str; default = { }; };
      path = lib.mkOption { type = lib.types.listOf lib.types.package; default = [ ]; };
      addressFamilies = lib.mkOption {
        type = lib.types.listOf lib.types.str;
        default = [ ];
        description = "Address families beyond AF_UNIX the client may use, e.g. [ \"AF_INET\" \"AF_INET6\" ] for one that reaches a network service.";
      };
      needsState = lib.mkOption {
        type = lib.types.bool;
        default = false;
        description = "Give the client a private StateDirectory at /var/lib/whatsapp-<name> (exposed as $STATE_DIRECTORY).";
      };
      environmentFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        description = "Optional EnvironmentFile (read by systemd as root, so it can hold secrets the client user cannot read directly). A missing file is tolerated.";
      };
    };
  };
in
{
  options.services.whatsapp = {
    enable = lib.mkEnableOption "the WhatsApp bridge, MCP server and clients";

    bridge = lib.mkOption {
      type = lib.types.package;
      description = "The whatsapp-bridge Go binary.";
    };

    pythonEnv = lib.mkOption {
      type = lib.types.package;
      description = "Python environment carrying the Python clients' runtime dependencies (mcp, httpx). Must provide `bin/python`.";
    };

    clientSource = lib.mkOption {
      type = lib.types.path;
      description = "whatsapp-client/, put on PYTHONPATH for every Python client.";
    };

    mcpSource = lib.mkOption {
      type = lib.types.path;
      description = "whatsapp-mcp-server/, run directly rather than installed as a package.";
    };

    forwarderSource = lib.mkOption {
      type = lib.types.path;
      description = "hindsight-forwarder/, run from source like the MCP server.";
    };

    hindsight = {
      enable = lib.mkEnableOption "the Hindsight forwarder client" // { default = true; };
      environmentFile = lib.mkOption {
        type = lib.types.path;
        default = "${cfg.stateDir}/hindsight.env";
        description = ''
          File holding HINDSIGHT_URL, HINDSIGHT_API_KEY, HINDSIGHT_BANK and any
          FORWARDER_* settings (see hindsight-forwarder/). Lives on the state
          volume, not in the image; the forwarder idles until it exists.
        '';
      };
    };

    stateDir = lib.mkOption {
      type = lib.types.path;
      default = "/data";
      description = ''
        Mountpoint holding the WhatsApp session and every message. Must match
        the flake's cattle.stateDir, which is what the container refuses to
        boot without.
      '';
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "whatsapp";
      description = "The account the bridge runs as, and the store's owner.";
    };

    mcpPort = lib.mkOption {
      type = lib.types.port;
      default = 8000;
      description = ''
        Port the MCP server listens on. Must match this service's `port` in
        services.yaml, which is also what the deploy health check polls.
      '';
    };

    allowedSources = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ "10.0.1.34" "10.0.1.31" "10.0.1.3" ];
      description = "Hosts allowed to reach mcpPort: gardenfacer (internal ingress), wallfacer (the border router, behind mcp-auth) and the hypervisor (the deploy health check).";
    };

    transcription = {
      enable = lib.mkEnableOption "local Whisper transcription of incoming voice notes" // { default = true; };
      model = lib.mkOption {
        type = lib.types.str;
        default = "base";
        description = "whisper.cpp model name (base, small, ...). The host is compute-constrained; upgrade here when it isn't.";
      };
    };

    clients = lib.mkOption {
      type = lib.types.attrsOf (lib.types.submodule clientOpts);
      default = { };
      description = "Extra local clients of the bridge, one hardened systemd unit each.";
    };
  };

  config = lib.mkIf cfg.enable {
    users.users.${cfg.user} = {
      isSystemUser = true;
      group = cfg.user;
      home = cfg.stateDir;
      description = "WhatsApp bridge";
      # Needed so the bridge can chgrp its socket to the clients group.
      extraGroups = [ clientsGroup ];
    };
    users.groups.${cfg.user} = { };
    users.groups.${clientsGroup} = { };

    systemd.tmpfiles.rules = [
      "d ${cfg.stateDir} 0750 ${cfg.user} ${cfg.user} -"
      "d ${storeDir}     0750 ${cfg.user} ${cfg.user} -"
      "d ${storeDir}/tmp 0750 ${cfg.user} ${cfg.user} -"
    ];

    systemd.services.whatsapp-bridge = {
      description = "WhatsApp bridge (Go, holds the session)";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];
      # ffmpeg converts outgoing voice notes to opus/ogg and decodes incoming
      # ones for whisper.
      path = [ pkgs.ffmpeg ] ++ lib.optional cfg.transcription.enable pkgs.whisper-cpp;

      environment = {
        WHATSAPP_STORE_DIR = storeDir;
        WHATSAPP_BRIDGE_ADDR = "unix:${socketPath}";
        WHATSAPP_BRIDGE_SOCKET_GROUP = clientsGroup;
      } // lib.optionalAttrs cfg.transcription.enable {
        WHATSAPP_TRANSCRIBE = "1";
        WHATSAPP_WHISPER_MODEL = "${storeDir}/models/ggml-${cfg.transcription.model}.bin";
      };

      serviceConfig = hardening // {
        User = cfg.user;
        Group = cfg.user;
        SupplementaryGroups = [ clientsGroup ];
        RuntimeDirectory = "whatsapp";
        RuntimeDirectoryMode = "0755";
        ReadWritePaths = [ storeDir ];
        RestrictAddressFamilies = [ "AF_UNIX" "AF_INET" "AF_INET6" ];
        WorkingDirectory = storeDir;
        # buildGoModule names the binary after go.mod's module path.
        ExecStart = "${cfg.bridge}/bin/whatsapp-client";
        Restart = "always";
        RestartSec = 5;
      };
    };

    # The MCP server is just another client, with an inbound port.
    services.whatsapp.clients.mcp = {
      description = "WhatsApp MCP server (Python, FastMCP over streamable-http)";
      user = "whatsapp-mcp";
      environment = {
        MCP_TRANSPORT = "streamable-http";
        MCP_HOST = "0.0.0.0";
        MCP_PORT = toString cfg.mcpPort;
        PYTHONPATH = "${cfg.clientSource}/src";
        # The source runs straight from the read-only store path.
        PYTHONDONTWRITEBYTECODE = "1";
      };
      addressFamilies = [ "AF_INET" "AF_INET6" ];
      execStart = "${cfg.pythonEnv}/bin/python ${cfg.mcpSource}/main.py";
    };

    services.whatsapp.clients.hindsight = lib.mkIf cfg.hindsight.enable {
      description = "WhatsApp to Hindsight forwarder";
      environment = {
        PYTHONPATH = "${cfg.clientSource}/src:${cfg.forwarderSource}/src";
        PYTHONDONTWRITEBYTECODE = "1";
      };
      # Reaches Hindsight over the network; the bridge over the socket.
      addressFamilies = [ "AF_INET" "AF_INET6" ];
      needsState = true;
      environmentFile = cfg.hindsight.environmentFile;
      execStart = "${cfg.pythonEnv}/bin/python -m hindsight_forwarder.main";
    };

    users.users = lib.mapAttrs' (name: client: lib.nameValuePair client.user {
      isSystemUser = true;
      group = client.user;
      description = client.description;
    }) cfg.clients;
    users.groups = lib.mapAttrs' (name: client: lib.nameValuePair client.user { }) cfg.clients;

    systemd.services = lib.mapAttrs' (name: client: lib.nameValuePair "whatsapp-${name}" (mkClientService name client)) cfg.clients;

    # allowedSources must include the hypervisor: it health-checks this port after every deploy, and blocking it triggers a rollback.
    networking.firewall.extraCommands = lib.concatMapStringsSep "\n"
      (src: "iptables -A nixos-fw -p tcp -s ${src} --dport ${toString cfg.mcpPort} -j nixos-fw-accept")
      cfg.allowedSources;
  };
}
