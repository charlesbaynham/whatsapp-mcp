{
  description = "WhatsApp bridge, MCP server and Hindsight forwarder, packaged as a cattle container. Two templates share this one app - `whatsapp` and `charlesbot-whatsapp` build the identical image, deployed as two separate containers each holding its own WhatsApp session and phone-number pairing.";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
  inputs.cattle.url = "git+https://github.com/charlesbaynham/nix-proxmox-cattle?ref=v1";

  outputs = { self, nixpkgs, cattle }:
    let
      system = "x86_64-linux";
      lib = nixpkgs.lib;
      pkgs = nixpkgs.legacyPackages.${system};

      # go.mod wants go >= 1.26.8; this pin's default `go` is 1.26.7.
      bridge = (pkgs.buildGoModule.override { go = pkgs.go_1_27; }) {
        pname = "whatsapp-bridge";
        version = "0.1.0";
        src = ./whatsapp-bridge;
        vendorHash = "sha256-iSAY8dD4JpKmcOQSIA1ffvdGemHn9vIExH8Kz+7sHZ0=";
        env.CGO_ENABLED = "1"; # go-sqlite3 is cgo.
      };

      # nixos-26.05 already carries mcp 1.26.0 (>=1.10,<2), so no overlay.
      # whatsapp-client itself is put on PYTHONPATH from source by the module.
      pythonEnv = pkgs.python3.withPackages (ps: [ ps.mcp ps.httpx ]);

      # The module carries no per-account state (nix/whatsapp.nix), so a second
      # WhatsApp account is just a second template under a different name -
      # everything that makes it a separate account (the session, the message
      # mirror) lives on the container's own state volume, not in this image.
      mkWhatsapp = name: cattle.lib.mkTemplate {
        inherit nixpkgs system name;
        stateDir = "/data";
        modules = [
          ./nix/whatsapp.nix
          {
            services.whatsapp = {
              enable = true;
              inherit bridge pythonEnv;
              clientSource = ./whatsapp-client;
              mcpSource = ./whatsapp-mcp-server;
              forwarderSource = ./hindsight-forwarder;
            };
          }
        ];
      };

      templates = {
        whatsapp = mkWhatsapp "whatsapp";
        "charlesbot-whatsapp" = mkWhatsapp "charlesbot-whatsapp";
      };
    in
    {
      nixosConfigurations =
        lib.foldl' lib.recursiveUpdate { } (map (t: t.nixosConfigurations) (lib.attrValues templates));

      # One attribute per service, named for it. mkTemplate calls every
      # template's own output `proxmoxLxcTemplate`, which cannot tell two of
      # them apart, so CI passes `attr: <service>`. The name must equal the
      # registry key in homelab-infra's services.yaml: it also becomes the
      # template filename prefix the deployer selects release assets by.
      packages.${system} =
        lib.mapAttrs (_: t: t.packages.${system}.proxmoxLxcTemplate) templates;
    };
}
