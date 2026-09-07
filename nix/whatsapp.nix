# WhatsApp MCP: a Go bridge holding the WhatsApp session, and a Python MCP
# server reading its message store. Both run as one user - they open the same
# SQLite files.
{ config, lib, pkgs, ... }:

let
  cfg = config.services.whatsapp;
  storeDir = "${cfg.stateDir}/store";

  # The MCP tools read and send any file under storeDir, so these units must
  # not be able to see anything else.
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
    RestrictAddressFamilies = [ "AF_UNIX" "AF_INET" "AF_INET6" ];
    SystemCallFilter = [ "@system-service" "~@privileged" "~@resources" ];
    SystemCallArchitectures = "native";
    CapabilityBoundingSet = "";
    AmbientCapabilities = "";
    UMask = "0077";
    ReadWritePaths = [ storeDir ];
  };
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

      environment = {
        WHATSAPP_STORE_DIR = storeDir;
        WHATSAPP_BRIDGE_ADDR = "127.0.0.1:${toString cfg.bridgePort}";
      };

      serviceConfig = hardening // {
        User = cfg.user;
        Group = cfg.user;
        WorkingDirectory = storeDir;
        # buildGoModule names the binary after go.mod's module path.
        ExecStart = "${cfg.bridge}/bin/whatsapp-client";
        Restart = "always";
        RestartSec = 5;
      };
    };

    systemd.services.whatsapp-mcp = {
      description = "WhatsApp MCP server (Python, FastMCP over streamable-http)";
      wantedBy = [ "multi-user.target" ];
      after = [ "network-online.target" "whatsapp-bridge.service" ];
      wants = [ "network-online.target" ];
      # ffmpeg converts outgoing voice notes to opus/ogg.
      path = [ pkgs.ffmpeg ];

      environment = {
        MCP_TRANSPORT = "streamable-http";
        MCP_HOST = "0.0.0.0";
        MCP_PORT = toString cfg.mcpPort;
        WHATSAPP_MESSAGES_DB = "${storeDir}/messages.db";
        WHATSAPP_STORE_DIR = storeDir;
        WHATSAPP_BRIDGE_URL = "http://127.0.0.1:${toString cfg.bridgePort}/api";
        # The source runs straight from the read-only store path.
        PYTHONDONTWRITEBYTECODE = "1";
      };

      serviceConfig = hardening // {
        User = cfg.user;
        Group = cfg.user;
        WorkingDirectory = storeDir;
        ExecStart = "${cfg.mcpEnv}/bin/python ${cfg.mcpSource}/main.py";
        Restart = "always";
        RestartSec = 5;
      };
    };

    # The bridge is loopback-only; only the MCP port needs a firewall rule.
    networking.firewall.allowedTCPPorts = [ cfg.mcpPort ];
  };
}
