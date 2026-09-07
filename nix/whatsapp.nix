# NixOS module for the WhatsApp MCP server: a Go bridge that holds the
# WhatsApp session and a Python MCP server that reads its message store,
# sharing one SQLite store on the state volume.
#
# Both processes run as the same user, because they open the same two SQLite
# files (whatsapp.db, the session — messages.db, every message). Splitting
# users would only mean fighting over file permissions on the same data.
{ config, lib, pkgs, ... }:

let
  cfg = config.services.whatsapp;
  storeDir = "${cfg.stateDir}/store";
in
{
  options.services.whatsapp = {
    enable = lib.mkEnableOption "the WhatsApp MCP bridge and server";

    bridge = lib.mkOption {
      type = lib.types.package;
      description = "The whatsapp-bridge Go binary.";
    };

    mcpEnv = lib.mkOption {
      type = lib.types.package;
      description = "Python environment carrying the MCP server's runtime dependencies (mcp, httpx, requests). Must provide `bin/python`.";
    };

    mcpSource = lib.mkOption {
      type = lib.types.path;
      description = "whatsapp-mcp-server/, run directly rather than installed as a package.";
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
      description = "The account both units run as, and the store's owner.";
    };

    bridgePort = lib.mkOption {
      type = lib.types.port;
      default = 8080;
      description = "Loopback port the Go bridge's REST API listens on.";
    };

    mcpPort = lib.mkOption {
      type = lib.types.port;
      default = 8000;
      description = ''
        Port the MCP server listens on. Must match this service's `port` in
        services.yaml, which is also what the deploy health check polls.
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    users.users.${cfg.user} = {
      isSystemUser = true;
      group = cfg.user;
      home = cfg.stateDir;
      description = "WhatsApp MCP bridge and server";
    };
    users.groups.${cfg.user} = { };

    # The mountpoint itself is created by Proxmox; the store directory beneath
    # it is the bridge's and the server's own.
    systemd.tmpfiles.rules = [
      "d ${cfg.stateDir}    0750 ${cfg.user} ${cfg.user} -"
      "d ${storeDir}        0750 ${cfg.user} ${cfg.user} -"
      # Converted voice notes land here before being sent - under storeDir, so
      # ReadWritePaths already covers it.
      "d ${storeDir}/tmp    0750 ${cfg.user} ${cfg.user} -"
    ];

    # ⚠️ No secrets file: unlike every other service on this pattern, there is
    # no app credential to seed. The WhatsApp session itself (whatsapp.db) IS
    # the secret, and it never leaves the store directory the units below are
    # sandboxed to. Access control is Traefik's basic auth at gardenfacer, on
    # its own state volume, not this one.

    # Hardened harder than the rest of the fleet: the MCP tools let a client
    # read and send any file under the store directory, so the unit must not
    # be able to see anything else worth having.
    systemd.services.whatsapp-bridge = {
      description = "WhatsApp bridge (Go, holds the session)";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" ];
      wants = [ "network-online.target" ];

      environment = {
        WHATSAPP_STORE_DIR = storeDir;
        WHATSAPP_BRIDGE_ADDR = "127.0.0.1:${toString cfg.bridgePort}";
      };

      serviceConfig = {
        User = cfg.user;
        Group = cfg.user;
        # The bridge still creates its own "store" subdirectory relative to
        # its working directory rather than reading WHATSAPP_STORE_DIR, until
        # the hosted-service branch lands — this WorkingDirectory is what
        # makes that land in the same place either way.
        WorkingDirectory = cfg.stateDir;
        # buildGoModule names the binary after the module path (go.mod's
        # `module whatsapp-client`), not the directory it lives in.
        ExecStart = "${cfg.bridge}/bin/whatsapp-client";
        Restart = "always";
        RestartSec = 5;

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
        RestrictSUIDSGID = true;
        RestrictRealtime = true;
        LockPersonality = true;
        RestrictNamespaces = true;
        RestrictAddressFamilies = [ "AF_UNIX" "AF_INET" "AF_INET6" ];
        ReadWritePaths = [ storeDir ];
      };
    };

    systemd.services.whatsapp-mcp = {
      description = "WhatsApp MCP server (Python, FastMCP over streamable-http)";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" "whatsapp-bridge.service" ];
      wants = [ "network-online.target" ];
      # ffmpeg converts outgoing voice notes to opus/ogg; not fatal if missing,
      # but the tool that needs it would otherwise fail with a bare ENOENT.
      path = [ pkgs.ffmpeg ];

      environment = {
        MCP_TRANSPORT = "streamable-http";
        MCP_HOST = "0.0.0.0";
        MCP_PORT = toString cfg.mcpPort;
        WHATSAPP_MESSAGES_DB = "${storeDir}/messages.db";
        WHATSAPP_STORE_DIR = storeDir;
        WHATSAPP_BRIDGE_URL = "http://127.0.0.1:${toString cfg.bridgePort}/api";
      };

      serviceConfig = {
        User = cfg.user;
        Group = cfg.user;
        WorkingDirectory = storeDir;
        ExecStart = "${cfg.mcpEnv}/bin/python ${cfg.mcpSource}/main.py";
        Restart = "always";
        RestartSec = 5;

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
        RestrictSUIDSGID = true;
        RestrictRealtime = true;
        LockPersonality = true;
        RestrictNamespaces = true;
        RestrictAddressFamilies = [ "AF_UNIX" "AF_INET" "AF_INET6" ];
        ReadWritePaths = [ storeDir ];
      };
    };

    # Only the MCP server is meant to be reached over the network — the bridge
    # is loopback-only and has no firewall rule to speak of.
    networking.firewall.allowedTCPPorts = [ cfg.mcpPort ];
  };
}
